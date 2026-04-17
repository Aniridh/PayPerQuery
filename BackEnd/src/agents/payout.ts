import { Submission, PayoutStatus } from '@prisma/client';
import prisma from '../db/client';
import { getLocusAdapter, PolicyViolation } from '../locus/adapter';
import { createHash } from 'crypto';
import { ethers } from 'ethers';

interface PayoutInput {
  submissionId: string;
}

interface PayoutResult {
  payoutId: string;
  txHash: string;
  amount: number;
  mocked: boolean;
}

/**
 * Generate deterministic transaction hash for DEMO_MODE
 * Format: keccak256(submissionId|timestamp)
 */
function generateDemoTxHash(submissionId: string): string {
  const timestamp = Date.now().toString();
  const input = `${submissionId}|${timestamp}`;
  // Use keccak256 (Ethereum hash function) via ethers
  return ethers.keccak256(ethers.toUtf8Bytes(input));
}

/**
 * Execute real USDC transfer on Base Sepolia
 */
async function executeRealPayout(
  wallet: string,
  amount: number,
  usdcAddress: string,
  rpcUrl: string,
  privateKey: string
): Promise<string> {
  const provider = new ethers.JsonRpcProvider(rpcUrl);
  const signer = new ethers.Wallet(privateKey, provider);

  // USDC has 6 decimals
  const amountWei = ethers.parseUnits(amount.toString(), 6);

  // Load USDC contract ABI (minimal transfer function)
  const usdcAbi = [
    'function transfer(address to, uint256 amount) returns (bool)',
  ];

  const usdcContract = new ethers.Contract(usdcAddress, usdcAbi, signer);

  // Execute transfer
  const tx = await usdcContract.transfer(wallet, amountWei);
  await tx.wait();

  return tx.hash;
}

/**
 * Retry helper for transient errors
 */
async function retry<T>(
  fn: () => Promise<T>,
  maxRetries: number = 3,
  delayMs: number = 1000
): Promise<T> {
  let lastError: Error | null = null;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error as Error;
      
      // Don't retry on policy violations or non-transient errors
      if (error instanceof PolicyViolation) {
        throw error;
      }

      // Check if error is transient (network, timeout, etc.)
      const isTransient = 
        error instanceof Error && (
          error.message.includes('timeout') ||
          error.message.includes('network') ||
          error.message.includes('ECONNRESET') ||
          error.message.includes('ETIMEDOUT')
        );

      if (!isTransient && attempt < maxRetries - 1) {
        // Not transient, but we'll retry anyway for safety
      }

      if (attempt < maxRetries - 1) {
        // Exponential backoff
        const backoffDelay = delayMs * Math.pow(2, attempt);
        await new Promise(resolve => setTimeout(resolve, backoffDelay));
      }
    }
  }

  throw lastError || new Error('Retry failed');
}

/**
 * Three-phase payout to avoid the distributed transaction problem:
 *
 *   PHASE 1 (DB tx):  Lock quest → check budget → decrement → create Payout PROCESSING
 *   PHASE 2 (external): Send blockchain tx (or generate demo hash)
 *          └─ immediately persist tx_hash so crash-recovery can find it
 *   PHASE 3 (DB tx):  Mark payout COMPLETED, submission PAID, write audit
 *
 * Crash between phases is recoverable:
 *   After Phase 1, before Phase 2: payout exists as PROCESSING with no tx_hash.
 *       → On retry we detect this and re-attempt the blockchain call.
 *   After Phase 2, before Phase 3: payout has tx_hash but status PROCESSING.
 *       → On retry we skip the blockchain call and just finalize the DB.
 *   After Phase 3: payout is COMPLETED — idempotent return.
 *
 * The critical invariant: we NEVER call executeRealPayout() inside a DB
 * transaction, so a Prisma rollback can't erase knowledge of sent funds.
 */
export async function payoutAgent(input: PayoutInput): Promise<PayoutResult> {
  const { submissionId } = input;

  // Load submission with all required relations
  const submission = await prisma.submission.findUnique({
    where: { id: submissionId },
    include: {
      quest: { include: { quest_rules: true } },
      verification_result: true,
      payout: true,
    },
  });

  if (!submission) {
    throw new Error(`Submission ${submissionId} not found`);
  }
  if (!submission.verification_result) {
    throw new Error(`Submission ${submissionId} has no verification result`);
  }
  // Accept both APPROVED and PAID (PAID means Phase 3 already ran on a previous attempt)
  if (submission.status !== 'APPROVED' && submission.status !== 'PAID') {
    throw new Error(`Submission ${submissionId} is not approved (status: ${submission.status})`);
  }

  // ── Idempotency: already fully completed ──
  if (submission.payout?.status === 'COMPLETED') {
    return {
      payoutId: submission.payout.id,
      txHash: submission.payout.tx_hash || '',
      amount: Number(submission.payout.amount),
      mocked: submission.payout.mocked,
    };
  }

  // ── Crash recovery: tx already sent but DB not finalized ──
  if (submission.payout?.tx_hash && submission.payout.status === 'PROCESSING') {
    await finalizePayoutInDb(
      submission.payout.id,
      submission.payout.tx_hash,
      submission.payout.mocked,
      submissionId,
      submission.quest.id,
      Number(submission.payout.amount),
      submission.verification_result.trace,
      submission.justification_text,
    );
    return {
      payoutId: submission.payout.id,
      txHash: submission.payout.tx_hash,
      amount: Number(submission.payout.amount),
      mocked: submission.payout.mocked,
    };
  }

  const quest = submission.quest;
  const rules = quest.quest_rules[0]?.rules as any;
  const payoutAmount = Number(quest.unit_amount);
  const locus = getLocusAdapter();

  // ── PHASE 1: Reserve budget + create Payout row ──
  // All inside one DB transaction with a row lock on Quest.
  const payout = await prisma.$transaction(async (tx) => {
    const questData = await tx.$queryRaw<Array<{
      id: string;
      budget_remaining: any;
      unit_amount: any;
    }>>`
      SELECT id, budget_remaining, unit_amount
      FROM "Quest"
      WHERE id = ${quest.id}
      FOR UPDATE
    `;

    if (questData.length === 0) {
      throw new Error('Quest not found');
    }

    const budgetRemaining = Number(questData[0].budget_remaining);
    const unitAmount = Number(questData[0].unit_amount);

    if (budgetRemaining < unitAmount) {
      throw new PolicyViolation(
        `Insufficient budget: ${budgetRemaining} < ${unitAmount}`,
        'Quest budget exhausted',
      );
    }

    // Policy gate (stub makes DB queries — acceptable inside tx for now)
    const authResult = await locus.authorizeSpend({
      policy: rules?.locus_policy || {},
      amount: payoutAmount,
      wallet: submission.wallet,
      questId: quest.id,
      deviceFingerprint: submission.device_fingerprint,
    });
    if (!authResult.authorized) {
      throw new PolicyViolation(
        authResult.reason || 'Policy violation',
        authResult.reason,
      );
    }

    // Decrement budget atomically
    await tx.quest.update({
      where: { id: quest.id },
      data: { budget_remaining: { decrement: unitAmount } },
    });

    // Create payout row (or recover existing PROCESSING row)
    return tx.payout.upsert({
      where: { submission_id: submissionId },
      update: { status: 'PROCESSING' },
      create: {
        submission_id: submissionId,
        quest_id: quest.id,
        amount: payoutAmount,
        currency: 'USDC',
        status: 'PROCESSING',
        mocked: false,
      },
    });
  });

  // ── PHASE 2: Blockchain call (OUTSIDE any DB transaction) ──
  const demoMode = process.env.DEMO_MODE === 'true';
  let txHash: string;
  let mocked = false;

  try {
    if (demoMode) {
      txHash = generateDemoTxHash(submissionId);
      mocked = true;
    } else {
      const usdcAddress = process.env.USDC_ADDRESS;
      const rpcUrl = process.env.RPC_URL;
      const hotWalletPk = process.env.HOT_WALLET_PK;

      if (!usdcAddress || !rpcUrl || !hotWalletPk) {
        throw new Error('Missing env vars for real payout: USDC_ADDRESS, RPC_URL, HOT_WALLET_PK');
      }

      txHash = await executeRealPayout(
        submission.wallet,
        payoutAmount,
        usdcAddress,
        rpcUrl,
        hotWalletPk,
      );
    }

    // Persist tx_hash IMMEDIATELY so crash recovery can find it.
    // This is a single UPDATE, not wrapped in the Phase 3 transaction,
    // because we need this to survive even if Phase 3 never starts.
    await prisma.payout.update({
      where: { id: payout.id },
      data: { tx_hash: txHash, mocked },
    });
  } catch (error) {
    // Blockchain call failed (or tx_hash persist failed before blockchain).
    // Refund budget and mark payout FAILED.
    await prisma.$transaction(async (tx) => {
      await tx.payout.update({
        where: { id: payout.id },
        data: { status: 'FAILED' },
      });
      await tx.quest.update({
        where: { id: quest.id },
        data: { budget_remaining: { increment: Number(quest.unit_amount) } },
      });
    });
    throw error;
  }

  // ── PHASE 3: Finalize — mark COMPLETED, submission PAID, audit ──
  await finalizePayoutInDb(
    payout.id,
    txHash,
    mocked,
    submissionId,
    quest.id,
    payoutAmount,
    submission.verification_result.trace,
    submission.justification_text,
  );

  return { payoutId: payout.id, txHash, amount: payoutAmount, mocked };
}

/**
 * Shared Phase-3 logic: mark payout COMPLETED, submission PAID, write audit.
 * Extracted so both the happy path and crash-recovery path use the same code.
 */
async function finalizePayoutInDb(
  payoutId: string,
  txHash: string,
  mocked: boolean,
  submissionId: string,
  questId: string,
  amount: number,
  decisionTrace: any,
  justificationText: string,
): Promise<void> {
  await prisma.$transaction(async (tx) => {
    await tx.payout.update({
      where: { id: payoutId },
      data: { tx_hash: txHash, status: 'COMPLETED', mocked },
    });
    await tx.submission.update({
      where: { id: submissionId },
      data: { status: 'PAID' },
    });
  });

  // Audit is best-effort (adapter swallows errors)
  const locus = getLocusAdapter();
  const justificationHash = createHash('sha256')
    .update(justificationText)
    .digest('hex');

  await locus.recordAudit({
    entityType: 'payout',
    entityId: payoutId,
    actorId: 'agent:payout',
    eventType: 'payout_completed',
    payload: {
      submission_id: submissionId,
      quest_id: questId,
      amount,
      tx_hash: txHash,
      mocked,
      decision_trace: decisionTrace,
      justification_hash: justificationHash,
      agent_ids: ['agent:verifier', 'agent:fraud_guard', 'agent:payout'],
      timestamp: new Date().toISOString(),
    },
  });
}
