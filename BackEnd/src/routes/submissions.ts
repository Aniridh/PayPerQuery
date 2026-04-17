import express from 'express';
import multer from 'multer';
import prisma from '../db/client';
import { createHash } from 'crypto';
import { createApiError, ErrorCode, generateRequestId } from '../utils/errors';
import * as fs from 'fs';
import * as path from 'path';

const router = express.Router();

// Configure multer for file uploads
const upload = multer({
  dest: 'uploads/raw/',
  limits: {
    fileSize: 10 * 1024 * 1024, // 10MB
  },
});

// Ensure uploads directory exists
const uploadsDir = path.join(__dirname, '../../uploads/raw');
if (!fs.existsSync(uploadsDir)) {
  fs.mkdirSync(uploadsDir, { recursive: true });
}

/**
 * POST /api/submissions
 * Create a new submission (async)
 */
router.post('/', upload.single('receipt_image'), async (req, res) => {
  const requestId = generateRequestId();

  try {
    const { quest_id, wallet, zip_prefix, justification_text } = req.body;

    if (!quest_id || !wallet || !zip_prefix || !justification_text) {
      return res.status(400).json(
        createApiError(ErrorCode.INVALID_INPUT, requestId, {
          missing: ['quest_id', 'wallet', 'zip_prefix', 'justification_text'].filter(
            field => !req.body[field]
          ),
        })
      );
    }

    if (!req.file) {
      return res.status(400).json(
        createApiError(ErrorCode.INVALID_INPUT, requestId, {
          message: 'receipt_image is required',
        })
      );
    }

    // Verify quest exists
    const quest = await prisma.quest.findUnique({
      where: { id: quest_id },
    });

    if (!quest) {
      return res.status(404).json(
        createApiError(ErrorCode.QUEST_NOT_FOUND, requestId)
      );
    }

    // Read image file
    const imageBuffer = fs.readFileSync(req.file.path);
    const contentHash = createHash('sha256').update(imageBuffer).digest('hex');

    // Generate device fingerprint: identifies the DEVICE, not the wallet.
    // wallet is intentionally excluded so that multiple wallets from the
    // same device/IP share a single fingerprint for velocity enforcement.
    const userAgent = req.get('user-agent') || '';
    const ip = req.ip || req.socket.remoteAddress || '';

    // Handle both IPv4 and IPv6 addresses
    let ipPrefix: string;
    if (ip.includes(':')) {
      // IPv6: use first 64 bits (first 4 groups) for /64 prefix
      const ipv6Parts = ip.split(':');
      ipPrefix = ipv6Parts.slice(0, 4).join(':');
    } else {
      // IPv4: use first 3 octets for /24 prefix
      ipPrefix = ip.split('.').slice(0, 3).join('.');
    }

    const deviceFingerprint = createHash('sha256')
      .update(`${userAgent}|${ipPrefix}`)
      .digest('hex');

    // Find or create contributor
    let contributor = await prisma.contributor.findUnique({
      where: { wallet },
    });

    if (!contributor) {
      contributor = await prisma.contributor.create({
        data: {
          wallet,
          device_fingerprint: deviceFingerprint,
        },
      });
    }

    // Create submission
    const submission = await prisma.submission.create({
      data: {
        quest_id,
        contributor_id: contributor.id,
        wallet,
        zip_prefix,
        justification_text,
        receipt_url: `/uploads/raw/${req.file.filename}`,
        content_hash: contentHash,
        device_fingerprint: deviceFingerprint,
        status: 'PENDING',
      },
    });

    // Create verification job
    await prisma.job.create({
      data: {
        type: 'VERIFY',
        entity_id: submission.id,
        status: 'QUEUED',
      },
    });

    res.status(201).json({
      submission_id: submission.id,
      status: 'PENDING',
      message: 'Submission created and queued for verification',
      requestId,
    });
  } catch (error) {
    console.error('Submission creation error:', error);
    res.status(500).json(
      createApiError(ErrorCode.INTERNAL_ERROR, requestId, {
        error: error instanceof Error ? error.message : String(error),
      })
    );
  }
});

/**
 * GET /api/submissions/:id/status
 * Get submission status, trace, and tx hash
 */
router.get('/:id/status', async (req, res) => {
  const requestId = generateRequestId();

  try {
    const { id } = req.params;

    const submission = await prisma.submission.findUnique({
      where: { id },
      include: {
        verification_result: true,
        payout: true,
      },
    });

    if (!submission) {
      return res.status(404).json(
        createApiError(ErrorCode.SUBMISSION_NOT_FOUND, requestId)
      );
    }

    // Redact internal decision trace from public endpoint to prevent
    // predicate oracle attacks (iterative probing to learn rule thresholds).
    // Full trace remains available via admin debug endpoint.
    const decision = submission.verification_result?.decision;
    let publicReason: string | null = null;
    if (decision === 'REJECT') {
      // Generic rejection reason — never expose which predicate failed or thresholds
      const reasons = submission.verification_result?.reasons || [];
      if (reasons.includes('duplicate_receipt') || reasons.includes('cross_wallet_duplicate') || reasons.includes('content_hash_duplicate')) {
        publicReason = 'This receipt has already been submitted.';
      } else if (reasons.some((r: string) => r.includes('velocity'))) {
        publicReason = 'Submission rate limit reached. Please try again later.';
      } else {
        publicReason = 'Submission did not meet quest eligibility requirements.';
      }
    }

    res.json({
      submission_id: submission.id,
      status: submission.status,
      decision: decision || null,
      reason: publicReason,
      tx_hash: submission.payout?.tx_hash || null,
      payout_id: submission.payout?.id || null,
      requestId,
    });
  } catch (error) {
    console.error('Status fetch error:', error);
    res.status(500).json(
      createApiError(ErrorCode.INTERNAL_ERROR, requestId)
    );
  }
});

export default router;

