import { JobType, JobStatus } from '@prisma/client';
import prisma from '../db/client';
import { verifierAgent } from '../agents/verifier';
import { fraudGuardAgent, calculateReceiptFingerprint } from '../agents/fraud-guard';

const WORKER_INTERVAL_MS = 750; // 500-1000ms range
const STALE_JOB_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes
const MAX_JOB_ATTEMPTS = 3;
let isRunning = false;
let workerInterval: NodeJS.Timeout | null = null;
let reaperInterval: NodeJS.Timeout | null = null;

/**
 * Claim next job using FOR UPDATE SKIP LOCKED.
 * SELECT + status UPDATE wrapped in a single transaction so the row lock
 * is held until the status flips to PROCESSING — prevents the race where
 * two workers both see the same QUEUED row.
 */
async function claimNextJob(): Promise<{ id: string; type: string; entity_id: string } | null> {
  return prisma.$transaction(async (tx) => {
    const result = await tx.$queryRaw<Array<{ id: string; type: string; entity_id: string }>>`
      SELECT id, type, entity_id
      FROM "Job"
      WHERE status = 'QUEUED'
      ORDER BY created_at ASC
      LIMIT 1
      FOR UPDATE SKIP LOCKED
    `;

    if (result.length === 0) {
      return null;
    }

    const job = result[0];

    await tx.job.update({
      where: { id: job.id },
      data: {
        status: 'PROCESSING',
        attempts: { increment: 1 },
      },
    });

    return job;
  });
}

/**
 * Reap stale jobs stuck in PROCESSING beyond the timeout.
 * Re-queues if under max attempts, otherwise marks FAILED.
 */
async function reapStaleJobs(): Promise<number> {
  const cutoff = new Date(Date.now() - STALE_JOB_TIMEOUT_MS);

  const staleJobs = await prisma.job.findMany({
    where: {
      status: 'PROCESSING',
      updated_at: { lt: cutoff },
    },
  });

  for (const job of staleJobs) {
    if (job.attempts >= MAX_JOB_ATTEMPTS) {
      await prisma.job.update({
        where: { id: job.id },
        data: {
          status: 'FAILED',
          last_error: `Reaped: stuck in PROCESSING for >${STALE_JOB_TIMEOUT_MS / 1000}s after ${job.attempts} attempts`,
        },
      });

      // Also mark the associated submission as FAILED so the user isn't stuck
      await prisma.submission.updateMany({
        where: { id: job.entity_id, status: { in: ['PENDING', 'QUEUED', 'PROCESSING'] } },
        data: { status: 'FAILED' },
      });
    } else {
      await prisma.job.update({
        where: { id: job.id },
        data: { status: 'QUEUED' },
      });
    }
  }

  if (staleJobs.length > 0) {
    console.log(`Reaped ${staleJobs.length} stale jobs`);
  }

  return staleJobs.length;
}

/**
 * Process verification job pipeline:
 * Verifier → FraudGuard → write verification_results → if APPROVE, enqueue PAYOUT job
 */
async function processVerificationJob(jobId: string, submissionId: string): Promise<void> {
  try {
    // Call Verifier Agent
    const verifierResult = await verifierAgent({ submissionId });

    // Call Fraud Guard Agent
    const fraudResult = await fraudGuardAgent({ submissionId, verifierResult });

    // Combine results
    const allPredicatesPass = verifierResult.rules_fired.every((r) => r.ok === true);
    const riskAcceptable = fraudResult.riskScore < 0.5;
    const decision = allPredicatesPass && riskAcceptable ? 'APPROVE' : 'REJECT';

    // Write verification results
    await prisma.verificationResult.upsert({
      where: { submission_id: submissionId },
      update: {
        decision,
        trace: {
          verifier: verifierResult,
          fraud_guard: fraudResult,
        },
        risk_score: fraudResult.riskScore,
        reasons: fraudResult.flags,
      },
      create: {
        submission_id: submissionId,
        decision,
        trace: {
          verifier: verifierResult,
          fraud_guard: fraudResult,
        },
        risk_score: fraudResult.riskScore,
        reasons: fraudResult.flags,
      },
    });

    // Calculate and store receipt_fingerprint for efficient duplicate detection
    const receiptFingerprint = calculateReceiptFingerprint(
      verifierResult.normalizedFields.merchant,
      verifierResult.normalizedFields.dateISO,
      verifierResult.normalizedFields.amountCents
    );

    // Update submission status and receipt_fingerprint
    await prisma.submission.update({
      where: { id: submissionId },
      data: {
        status: decision === 'APPROVE' ? 'APPROVED' : 'REJECTED',
        receipt_fingerprint: receiptFingerprint,
      },
    });

    // If approved, enqueue PAYOUT job — but only if one doesn't already exist
    // (guards against duplicate VERIFY jobs from the race in scenario 3)
    if (decision === 'APPROVE') {
      const existingPayoutJob = await prisma.job.findFirst({
        where: {
          type: 'PAYOUT',
          entity_id: submissionId,
        },
      });

      if (!existingPayoutJob) {
        await prisma.job.create({
          data: {
            type: 'PAYOUT',
            entity_id: submissionId,
            status: 'QUEUED',
          },
        });
      }
    }

    // Job completion is handled in processJob
  } catch (error) {
    // Mark submission FAILED — but only if it hasn't already advanced to
    // APPROVED/PAID. Without this guard, a partition between writing the
    // VerificationResult (APPROVE) and updating Submission (APPROVED)
    // would cause the catch block to overwrite APPROVED with FAILED.
    await prisma.submission.updateMany({
      where: {
        id: submissionId,
        status: { in: ['PENDING', 'QUEUED', 'PROCESSING'] },
      },
      data: {
        status: 'FAILED',
      },
    });

    throw error;
  }
}

/**
 * Process payout job
 */
async function processPayoutJob(jobId: string, submissionId: string): Promise<void> {
  const { payoutAgent } = await import('../agents/payout');
  await payoutAgent({ submissionId });
  // Job completion is handled in processJob
}

/**
 * Process a single job
 */
async function processJob(jobId: string, jobType: JobType, entityId: string): Promise<void> {
  try {
    switch (jobType) {
      case 'VERIFY':
        await processVerificationJob(jobId, entityId);
        break;
      case 'PAYOUT':
        await processPayoutJob(jobId, entityId);
        break;
      default:
        throw new Error(`Unknown job type: ${jobType}`);
    }

    // Mark job as completed on success
    await prisma.job.update({
      where: { id: jobId },
      data: {
        status: 'COMPLETED',
      },
    });
  } catch (error) {
    // On failure: increment attempts, set FAILED with last_error
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    
    await prisma.job.update({
      where: { id: jobId },
      data: {
        status: 'FAILED',
        last_error: errorMessage,
      },
    });

    throw error;
  }
}

/**
 * Worker loop: claim and process one job
 */
async function workerTick(): Promise<void> {
  if (isRunning) {
    return; // Skip if already processing
  }

  try {
    isRunning = true;
    const job = await claimNextJob();

    if (!job) {
      // No jobs available, wait for next tick
      return;
    }

    await processJob(job.id, job.type as JobType, job.entity_id);
  } catch (error) {
    console.error('Worker error:', error);
    // Error already handled in processJob, just log here
  } finally {
    isRunning = false;
  }
}

/**
 * Start the worker loop and stale-job reaper
 */
export function startWorker(): void {
  if (workerInterval) {
    console.log('Worker already running');
    return;
  }

  console.log('Starting worker...');

  workerInterval = setInterval(() => {
    workerTick().catch((error) => {
      console.error('Unhandled worker error:', error);
    });
  }, WORKER_INTERVAL_MS);

  // Reaper runs every 60s to reclaim stuck PROCESSING jobs
  reaperInterval = setInterval(() => {
    reapStaleJobs().catch((error) => {
      console.error('Reaper error:', error);
    });
  }, 60_000);

  // Process immediately on start
  workerTick().catch((error) => {
    console.error('Initial worker tick error:', error);
  });
}

/**
 * Stop the worker loop and reaper
 */
export function stopWorker(): void {
  if (workerInterval) {
    clearInterval(workerInterval);
    workerInterval = null;
  }
  if (reaperInterval) {
    clearInterval(reaperInterval);
    reaperInterval = null;
  }
  console.log('Worker stopped');
}
