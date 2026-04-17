import { Submission } from '@prisma/client';
import prisma from '../db/client';
import { createHash } from 'crypto';

interface FraudGuardInput {
  submissionId: string;
  verifierResult: {
    rules_fired: Array<{ field: string; ok: boolean; observed: any }>;
    normalizedFields: {
      merchant: string;
      dateISO: string;
      amountCents: number;
    };
    confidence: number;
  };
}

interface FraudGuardResult {
  riskScore: number;
  flags: string[];
  qualityScore?: number;
}

/**
 * Calculate canonicalized receipt fingerprint
 * Format: sha256(lower(merchant)|'|'|dateISO|'|'|amountCents)
 */
export function calculateReceiptFingerprint(
  merchant: string,
  dateISO: string,
  amountCents: number
): string {
  const normalizedMerchant = merchant.toLowerCase().trim();
  const normalizedDate = dateISO.split('T')[0]; // ISO date only
  const canonical = `${normalizedMerchant}|${normalizedDate}|${amountCents}`;
  return createHash('sha256').update(canonical).digest('hex');
}

/**
 * Calculate fuzzy fingerprint (merchant + date only)
 */
function calculateFuzzyFingerprint(merchant: string, dateISO: string): string {
  const normalizedMerchant = merchant.toLowerCase().trim();
  const normalizedDate = dateISO.split('T')[0];
  const canonical = `${normalizedMerchant}|${normalizedDate}`;
  return createHash('sha256').update(canonical).digest('hex');
}

/**
 * Calculate contributor key: wallet + FPJS + IP /24
 */
function calculateContributorKey(
  wallet: string,
  deviceFingerprint: string,
  ipPrefix: string
): string {
  return createHash('sha256')
    .update(`${wallet}|${deviceFingerprint}|${ipPrefix}`)
    .digest('hex');
}

/**
 * Check justification quality (if enabled)
 */
function checkJustificationQuality(
  justification: string,
  enableQualityScoring: boolean
): { score: number; flags: string[] } {
  if (!enableQualityScoring) {
    return { score: 1.0, flags: [] };
  }

  const flags: string[] = [];
  let score = 1.0;

  // Min words check (15 words)
  const words = justification.trim().split(/\s+/).filter(w => w.length > 0);
  if (words.length < 15) {
    flags.push('too_short');
    score -= 0.3;
  }

  // Banned phrases
  const bannedPhrases = [
    'this is a test',
    'just testing',
    'demo submission',
    'test receipt',
    'placeholder',
    'sample text',
  ];
  const lowerJustification = justification.toLowerCase();
  for (const phrase of bannedPhrases) {
    if (lowerJustification.includes(phrase)) {
      flags.push('banned_phrase');
      score -= 0.4;
      break;
    }
  }

  // Keyword presence (pet-related)
  const petKeywords = ['dog', 'cat', 'pet', 'puppy', 'kitten', 'food', 'toy', 'treat', 'animal'];
  const hasPetKeyword = petKeywords.some(keyword => 
    lowerJustification.includes(keyword)
  );
  if (!hasPetKeyword) {
    flags.push('no_keyword');
    score -= 0.2;
  }

  // Similarity check (simple Jaccard similarity vs last N)
  // For demo, we'll skip this as it requires storing previous justifications
  // In production, compare against last N justifications

  return {
    score: Math.max(0, Math.min(1, score)),
    flags,
  };
}

export async function fraudGuardAgent(
  input: FraudGuardInput
): Promise<FraudGuardResult> {
  const { submissionId, verifierResult } = input;

  // Load submission
  const submission = await prisma.submission.findUnique({
    where: { id: submissionId },
    include: {
      verification_result: true,
    },
  });

  if (!submission) {
    throw new Error(`Submission ${submissionId} not found`);
  }

  const flags: string[] = [];
  let riskScore = 0.0;

  const { merchant, dateISO, amountCents } = verifierResult.normalizedFields;

  // 1. Canonical fingerprint: sha256(lower(merchant)|'|'|dateISO|'|'|amountCents)
  const receiptFingerprint = calculateReceiptFingerprint(merchant, dateISO, amountCents);

  // 2a. Same-wallet duplicate: hard block
  const sameWalletDuplicate = await prisma.submission.findFirst({
    where: {
      wallet: submission.wallet,
      receipt_fingerprint: receiptFingerprint,
      id: { not: submissionId },
      status: { in: ['APPROVED', 'PAID'] },
    },
  });

  if (sameWalletDuplicate) {
    flags.push('duplicate_receipt');
    riskScore = 1.0;
    return { riskScore, flags };
  }

  // 2b. Cross-wallet duplicate: same receipt submitted by ANY wallet → hard block
  // Prevents receipt farming across multiple wallets
  const crossWalletDuplicate = await prisma.submission.findFirst({
    where: {
      receipt_fingerprint: receiptFingerprint,
      id: { not: submissionId },
      wallet: { not: submission.wallet },
      status: { in: ['APPROVED', 'PAID'] },
    },
  });

  if (crossWalletDuplicate) {
    flags.push('cross_wallet_duplicate');
    riskScore = 1.0;
    return { riskScore, flags };
  }

  // 2c. Content-hash duplicate: same image file submitted by any wallet
  // Catches byte-identical images even if OCR fields differ
  const contentHashDuplicate = await prisma.submission.findFirst({
    where: {
      content_hash: submission.content_hash,
      id: { not: submissionId },
      status: { in: ['APPROVED', 'PAID'] },
    },
  });

  if (contentHashDuplicate) {
    flags.push('content_hash_duplicate');
    riskScore = 1.0;
    return { riskScore, flags };
  }

  // 3. Warn if same (merchant, dateISO) within 7 days
  const sevenDaysAgo = new Date();
  sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);

  const fuzzyFingerprint = calculateFuzzyFingerprint(merchant, dateISO);
  
  const similarSubmissions = await prisma.submission.findMany({
    where: {
      wallet: submission.wallet,
      id: { not: submissionId },
      created_at: { gte: sevenDaysAgo },
      status: { in: ['APPROVED', 'PAID'] },
    },
    include: {
      verification_result: true,
    },
  });

  for (const similarSub of similarSubmissions) {
    if (similarSub.verification_result?.trace) {
      const trace = similarSub.verification_result.trace as any;
      const similarFields = trace.verifier?.normalizedFields || trace.verifier?.ocr_fields || trace.normalizedFields || trace.ocr_fields;
      
      if (similarFields) {
        const similarDate = similarFields.dateISO || similarFields.date;
        const similarMerchant = similarFields.merchant;
        const similarFuzzy = calculateFuzzyFingerprint(similarMerchant, similarDate);
        
        if (similarFuzzy === fuzzyFingerprint) {
          flags.push('similar_receipt_pattern');
          riskScore = Math.max(riskScore, 0.5);
          break;
        }
      }
    }
  }

  // 4. Velocity limits (device_fingerprint no longer contains wallet,
  //    so this correctly rate-limits across all wallets from one device)
  const today = new Date();
  today.setHours(0, 0, 0, 0);

  // 4a. Per-device velocity: ≤3 approvals/day across ALL wallets on this device
  const deviceApprovals = await prisma.submission.count({
    where: {
      device_fingerprint: submission.device_fingerprint,
      quest_id: submission.quest_id,
      status: { in: ['APPROVED', 'PAID'] },
      created_at: { gte: today },
    },
  });

  if (deviceApprovals >= 3) {
    flags.push('device_velocity_exceeded');
    riskScore = Math.max(riskScore, 0.7);
  }

  // 4b. Per-wallet velocity: ≤3 approvals/day per wallet (unchanged semantics)
  const walletApprovals = await prisma.submission.count({
    where: {
      wallet: submission.wallet,
      quest_id: submission.quest_id,
      status: { in: ['APPROVED', 'PAID'] },
      created_at: { gte: today },
    },
  });

  if (walletApprovals >= 3) {
    flags.push('wallet_velocity_exceeded');
    riskScore = Math.max(riskScore, 0.5);
  }

  // 4c. Sybil signal: multiple distinct wallets from same device on same quest
  const distinctWalletsOnDevice = await prisma.submission.groupBy({
    by: ['wallet'],
    where: {
      device_fingerprint: submission.device_fingerprint,
      quest_id: submission.quest_id,
      created_at: { gte: today },
    },
  });

  if (distinctWalletsOnDevice.length > 2) {
    flags.push('sybil_multi_wallet_device');
    riskScore = Math.max(riskScore, 0.8);
  }

  // 5. Quality scoring (if enabled)
  const enableQualityScoring = process.env.ENABLE_QUALITY_SCORING === 'true';
  const qualityCheck = checkJustificationQuality(
    submission.justification_text,
    enableQualityScoring
  );

  let qualityScore: number | undefined;
  if (enableQualityScoring) {
    qualityScore = qualityCheck.score;
    if (qualityCheck.flags.length > 0) {
      flags.push(...qualityCheck.flags.map(f => `quality_${f}`));
      // In demo, don't block on quality, just record
      // riskScore = Math.max(riskScore, 0.3);
    }
  }

  return {
    riskScore,
    flags,
    qualityScore,
  };
}
