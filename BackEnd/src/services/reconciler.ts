import prisma from '../db/client';

/**
 * Startup reconciliation job.
 *
 * After a Postgres partition (or process crash), the system can be left in
 * several inconsistent states. This reconciler detects and heals each one.
 *
 * DESIGN PRINCIPLES:
 *
 *   1. Every operation is idempotent — running it twice produces the same
 *      result. This means the reconciler is safe to run on every startup
 *      even if nothing is broken.
 *
 *   2. Every operation is monotonic — it only moves state forward toward
 *      terminal states, never backward. APPROVED → PAID is fine;
 *      we never move PAID → APPROVED.
 *
 *   3. We never send money. The reconciler only heals DB state.
 *      If money needs to be sent, we re-enqueue a PAYOUT job and let the
 *      normal pipeline handle it with its own idempotency guards.
 *
 *   4. We log every action for post-incident audit.
 */

interface ReconcileResult {
  orphanedSubmissions: number;
  staleProcessingJobs: number;
  mismarkedSubmissions: number;
  approvedWithoutPayoutJob: number;
  payoutsCompletedButSubmissionNotPaid: number;
  payoutsProcessingWithTxHash: number;
  payoutsProcessingWithoutTxHash: number;
  budgetDriftCorrected: number;
}

export async function reconcile(): Promise<ReconcileResult> {
  const result: ReconcileResult = {
    orphanedSubmissions: 0,
    staleProcessingJobs: 0,
    mismarkedSubmissions: 0,
    approvedWithoutPayoutJob: 0,
    payoutsCompletedButSubmissionNotPaid: 0,
    payoutsProcessingWithTxHash: 0,
    payoutsProcessingWithoutTxHash: 0,
    budgetDriftCorrected: 0,
  };

  console.log('[reconciler] Starting reconciliation...');

  // ─────────────────────────────────────────────────────────────────
  // 1. ORPHANED SUBMISSIONS: Submission exists with PENDING status
  //    but no corresponding Job row.
  //
  //    HOW THIS HAPPENS:
  //    POST /api/submissions creates the Submission row (line 102),
  //    then creates the Job row (line 117). If Postgres goes down
  //    between those two writes, the Submission exists but no Job
  //    will ever pick it up.
  //
  //    WHY THE FIX IS SAFE:
  //    We create a VERIFY job for the orphaned submission. If a Job
  //    already exists (race with normal operation), the worker's
  //    claimNextJob uses FOR UPDATE SKIP LOCKED — worst case the
  //    duplicate job runs and the verifier's upsert on
  //    VerificationResult is idempotent.
  // ─────────────────────────────────────────────────────────────────
  const orphanedSubmissions = await prisma.submission.findMany({
    where: {
      status: 'PENDING',
    },
    select: { id: true },
  });

  for (const sub of orphanedSubmissions) {
    const hasJob = await prisma.job.findFirst({
      where: { entity_id: sub.id, type: 'VERIFY' },
    });

    if (!hasJob) {
      await prisma.job.create({
        data: {
          type: 'VERIFY',
          entity_id: sub.id,
          status: 'QUEUED',
        },
      });
      console.log(`[reconciler] Created missing VERIFY job for submission ${sub.id}`);
      result.orphanedSubmissions++;
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // 2. STALE PROCESSING JOBS: Job stuck in PROCESSING.
  //
  //    HOW THIS HAPPENS:
  //    Worker claims job (status → PROCESSING), then Postgres goes
  //    down mid-execution. The in-memory work is lost, and the job
  //    stays PROCESSING forever.
  //
  //    WHY THE FIX IS SAFE:
  //    Re-queuing a PROCESSING job means the worker will re-run it.
  //    - VERIFY jobs: verifierAgent reads submission + runs OCR
  //      (cached) + upserts VerificationResult. All idempotent.
  //    - PAYOUT jobs: payoutAgent has three-phase design with
  //      explicit crash recovery at the top. Re-running is safe.
  //
  //    We only re-queue if attempts < MAX. Otherwise we FAIL it
  //    so it doesn't loop forever.
  // ─────────────────────────────────────────────────────────────────
  const MAX_ATTEMPTS = 3;
  const staleJobs = await prisma.job.findMany({
    where: { status: 'PROCESSING' },
  });

  for (const job of staleJobs) {
    if (job.attempts >= MAX_ATTEMPTS) {
      await prisma.job.update({
        where: { id: job.id },
        data: {
          status: 'FAILED',
          last_error: 'Reconciler: exceeded max attempts while stuck in PROCESSING',
        },
      });

      // Also fail the submission so the user isn't stuck polling
      await prisma.submission.updateMany({
        where: {
          id: job.entity_id,
          status: { in: ['PENDING', 'QUEUED', 'PROCESSING'] },
        },
        data: { status: 'FAILED' },
      });

      console.log(`[reconciler] Failed stale job ${job.id} (${job.attempts} attempts)`);
    } else {
      await prisma.job.update({
        where: { id: job.id },
        data: { status: 'QUEUED' },
      });
      console.log(`[reconciler] Re-queued stale PROCESSING job ${job.id}`);
    }
    result.staleProcessingJobs++;
  }

  // ─────────────────────────────────────────────────────────────────
  // 3. VERIFIED-BUT-MISMARKED SUBMISSIONS:
  //    VerificationResult says APPROVE, but Submission.status is
  //    FAILED or PENDING (not APPROVED), and no COMPLETED payout.
  //
  //    HOW THIS HAPPENS:
  //    processVerificationJob upserts VerificationResult with
  //    decision APPROVE, then tries to update Submission to
  //    APPROVED. If Postgres partitions between those writes, the
  //    catch block fires and marks Submission FAILED (or if the
  //    catch also fails, it stays PENDING). Either way, the
  //    verification work is done — the VerificationResult proves
  //    the submission should be APPROVED.
  //
  //    WHY THE FIX IS SAFE:
  //    We promote Submission status to APPROVED. The VerificationResult
  //    is the source of truth for the decision. Moving a FAILED or
  //    PENDING submission to APPROVED when the verification already
  //    concluded APPROVE is restoring the correct state, not making
  //    a new decision. Then we create a PAYOUT job if needed.
  //    payoutAgent handles duplicates via its Payout upsert.
  // ─────────────────────────────────────────────────────────────────
  const mismarkedSubmissions = await prisma.submission.findMany({
    where: {
      status: { in: ['FAILED', 'PENDING'] },
      verification_result: {
        decision: 'APPROVE',
      },
      payout: null, // No completed payout
    },
    select: { id: true, status: true },
  });

  for (const sub of mismarkedSubmissions) {
    await prisma.submission.update({
      where: { id: sub.id },
      data: { status: 'APPROVED' },
    });
    console.log(`[reconciler] Promoted submission ${sub.id} from ${sub.status} to APPROVED (VerificationResult says APPROVE)`);
    result.mismarkedSubmissions++;
  }

  // Now find all APPROVED submissions without a payout job (includes
  // ones we just promoted above + ones that were already APPROVED)
  const approvedWithoutPayout = await prisma.submission.findMany({
    where: {
      status: 'APPROVED',
      payout: null,
      verification_result: {
        decision: 'APPROVE',
      },
    },
    select: { id: true },
  });

  for (const sub of approvedWithoutPayout) {
    const hasPayoutJob = await prisma.job.findFirst({
      where: { entity_id: sub.id, type: 'PAYOUT' },
    });

    if (!hasPayoutJob) {
      await prisma.job.create({
        data: {
          type: 'PAYOUT',
          entity_id: sub.id,
          status: 'QUEUED',
        },
      });
      console.log(`[reconciler] Created missing PAYOUT job for approved submission ${sub.id}`);
      result.approvedWithoutPayoutJob++;
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // 4. PAYOUT COMPLETED BUT SUBMISSION NOT PAID:
  //    Payout.status = COMPLETED with a tx_hash, but
  //    Submission.status is still APPROVED (not PAID).
  //
  //    HOW THIS HAPPENS:
  //    finalizePayoutInDb runs a transaction that updates both
  //    Payout and Submission. But if the process crashed after
  //    the Phase-2 tx_hash persist and before Phase 3, a
  //    subsequent payout retry would have run Phase-3. However
  //    if the retry itself was interrupted between updating Payout
  //    to COMPLETED and updating Submission... or if the whole
  //    transaction committed on the Postgres side but the TCP
  //    ACK was lost (Prisma sees a timeout, Postgres committed).
  //
  //    Actually the more common cause: Phase 3's transaction
  //    commits Payout → COMPLETED and Submission → PAID atomically.
  //    But the Locus audit event after it could fail and throw,
  //    and if the error propagates up, the job gets marked FAILED
  //    even though the DB state is correct. On re-run, payoutAgent
  //    sees COMPLETED and returns early. But the Job still shows
  //    FAILED. The submission *is* PAID in this case. So this
  //    check covers the truly exotic partition case.
  //
  //    WHY THE FIX IS SAFE:
  //    Moving Submission from APPROVED → PAID when a COMPLETED
  //    payout with tx_hash exists is monotonic. The money was
  //    already sent. We're just aligning the submission status
  //    to reflect reality. Running this twice is a no-op (already PAID).
  // ─────────────────────────────────────────────────────────────────
  const completedPayoutsWithWrongSubmission = await prisma.payout.findMany({
    where: {
      status: 'COMPLETED',
      tx_hash: { not: null },
      submission: {
        status: { not: 'PAID' },
      },
    },
    select: {
      id: true,
      submission_id: true,
      tx_hash: true,
    },
  });

  for (const p of completedPayoutsWithWrongSubmission) {
    await prisma.submission.update({
      where: { id: p.submission_id },
      data: { status: 'PAID' },
    });
    console.log(`[reconciler] Marked submission ${p.submission_id} as PAID (payout ${p.id} already COMPLETED with tx ${p.tx_hash})`);
    result.payoutsCompletedButSubmissionNotPaid++;
  }

  // ─────────────────────────────────────────────────────────────────
  // 5. PAYOUT PROCESSING WITH TX_HASH (Phase-2 completed, Phase-3 didn't):
  //    Payout.status = PROCESSING, tx_hash is set.
  //
  //    HOW THIS HAPPENS:
  //    payout.ts Phase 2 succeeded (blockchain tx sent), the
  //    immediate tx_hash persist succeeded (line 275), then
  //    Postgres went down before Phase 3's transaction could commit.
  //
  //    WHY THE FIX IS SAFE:
  //    The money was already sent (tx_hash proves it). We just
  //    need to finalize: Payout → COMPLETED, Submission → PAID.
  //    This is exactly what finalizePayoutInDb does, and
  //    payoutAgent's crash recovery path does the same. But if
  //    there's no PAYOUT job queued to trigger that path, we
  //    do it directly here. Running it twice: Payout is already
  //    COMPLETED, Submission is already PAID ��� both updates are
  //    no-ops at the DB level.
  // ─────────────────────────────────────────────────────────────────
  const payoutsNeedingFinalization = await prisma.payout.findMany({
    where: {
      status: 'PROCESSING',
      tx_hash: { not: null },
    },
    include: {
      submission: {
        include: { verification_result: true },
      },
    },
  });

  for (const p of payoutsNeedingFinalization) {
    await prisma.$transaction(async (tx) => {
      await tx.payout.update({
        where: { id: p.id },
        data: { status: 'COMPLETED' },
      });
      await tx.submission.update({
        where: { id: p.submission_id },
        data: { status: 'PAID' },
      });
    });

    // Best-effort audit
    try {
      await prisma.auditEvent.create({
        data: {
          entity_type: 'payout',
          entity_id: p.id,
          actor_id: 'reconciler',
          event_type: 'payout_finalized_by_reconciler',
          payload: {
            submission_id: p.submission_id,
            tx_hash: p.tx_hash,
            reason: 'Phase-3 did not complete before partition/crash',
          },
        },
      });
    } catch {
      // Audit failure is non-blocking
    }

    console.log(`[reconciler] Finalized payout ${p.id} (tx_hash present, was stuck in PROCESSING)`);
    result.payoutsProcessingWithTxHash++;
  }

  // ─────────────────────────────────────────────────────────────────
  // 6. PAYOUT PROCESSING WITHOUT TX_HASH (Phase-1 completed, Phase-2 didn't):
  //    Payout.status = PROCESSING, tx_hash is null.
  //
  //    HOW THIS HAPPENS:
  //    payout.ts Phase 1 committed (budget decremented, Payout row
  //    created), then Postgres or the process died before Phase 2
  //    could send the blockchain tx.
  //
  //    WHY THE FIX IS SAFE:
  //    No money was sent (no tx_hash). We re-enqueue a PAYOUT job.
  //    When payoutAgent runs, it loads the submission, sees the
  //    payout with status PROCESSING and no tx_hash, falls through
  //    to Phase 1's upsert (which just sets status back to
  //    PROCESSING — no-op), then attempts Phase 2. Budget was
  //    already decremented in Phase 1, and the upsert re-uses the
  //    existing Payout row, so no double decrement.
  //
  //    WAIT — there IS a subtlety. Phase 1 does:
  //      1. Lock Quest with FOR UPDATE
  //      2. Check budget
  //      3. Decrement budget
  //      4. Upsert Payout
  //    If the Payout already exists (from the interrupted run),
  //    the upsert hits the UPDATE branch (status → PROCESSING).
  //    But step 3 decrements budget AGAIN. That's a double
  //    decrement.
  //
  //    So we can't just re-enqueue blindly. Instead: we mark the
  //    payout as FAILED and REFUND the budget, then create a fresh
  //    PAYOUT job. payoutAgent will create a new Payout row from
  //    scratch via Phase 1.
  // ─────────────────────────────────────────────────────────────────
  const payoutsNeedingRetry = await prisma.payout.findMany({
    where: {
      status: 'PROCESSING',
      tx_hash: null,
    },
    select: {
      id: true,
      submission_id: true,
      quest_id: true,
      amount: true,
    },
  });

  for (const p of payoutsNeedingRetry) {
    // Refund budget and mark payout FAILED — atomically
    await prisma.$transaction(async (tx) => {
      await tx.payout.update({
        where: { id: p.id },
        data: { status: 'FAILED' },
      });
      await tx.quest.update({
        where: { id: p.quest_id },
        data: { budget_remaining: { increment: Number(p.amount) } },
      });
    });

    // Now enqueue a fresh PAYOUT job (if one isn't already queued)
    const existingJob = await prisma.job.findFirst({
      where: {
        entity_id: p.submission_id,
        type: 'PAYOUT',
        status: { in: ['QUEUED', 'PROCESSING'] },
      },
    });

    if (!existingJob) {
      await prisma.job.create({
        data: {
          type: 'PAYOUT',
          entity_id: p.submission_id,
          status: 'QUEUED',
        },
      });
    }

    // Best-effort audit
    try {
      await prisma.auditEvent.create({
        data: {
          entity_type: 'payout',
          entity_id: p.id,
          actor_id: 'reconciler',
          event_type: 'payout_reset_by_reconciler',
          payload: {
            submission_id: p.submission_id,
            quest_id: p.quest_id,
            amount: Number(p.amount),
            reason: 'Phase-2 never started — budget refunded, fresh PAYOUT job created',
          },
        },
      });
    } catch {
      // Audit failure is non-blocking
    }

    console.log(`[reconciler] Reset payout ${p.id} (no tx_hash) — refunded budget, re-enqueued`);
    result.payoutsProcessingWithoutTxHash++;
  }

  // ─────────────────────────────────────────────────────────────────
  // 7. BUDGET DRIFT: Quest.budget_remaining doesn't match
  //    budget_total minus sum of COMPLETED payouts.
  //
  //    HOW THIS HAPPENS:
  //    Phase 1 decrements budget. If payout fails and the
  //    compensation transaction (budget refund) doesn't commit
  //    (partition right there), budget is decremented but no
  //    payout exists. Or a reconciler run above refunded budget
  //    but then crashed before the audit write — on next run the
  //    payout is already FAILED so it won't be caught by check 6,
  //    but the budget might be off.
  //
  //    Also: the previous payout code (before our fix) did
  //    blockchain calls inside the DB transaction. If it ever ran
  //    in production, budget could be arbitrarily wrong.
  //
  //    WHY THE FIX IS SAFE:
  //    budget_remaining should equal budget_total - sum(COMPLETED payouts)
  //    - sum(PROCESSING payouts with no tx_hash, i.e. reserved but not sent).
  //
  //    Wait — after check 6 above, there are no PROCESSING payouts
  //    without tx_hash anymore (they were all refunded). And check 5
  //    finalized all PROCESSING payouts WITH tx_hash to COMPLETED.
  //    So at this point, the only payouts that matter are COMPLETED.
  //
  //    Recalculating from COMPLETED payouts is the source of truth.
  //    This is safe because we're computing an accounting identity,
  //    not making a business decision.
  // ─────────────────────────────────────────────────────────────────
  const quests = await prisma.quest.findMany({
    where: { status: 'ACTIVE' },
    select: { id: true, budget_total: true, budget_remaining: true },
  });

  for (const quest of quests) {
    const completedPayouts = await prisma.payout.aggregate({
      where: {
        quest_id: quest.id,
        status: 'COMPLETED',
      },
      _sum: { amount: true },
    });

    // Also account for PROCESSING payouts (reserved budget, not yet sent)
    const processingPayouts = await prisma.payout.aggregate({
      where: {
        quest_id: quest.id,
        status: 'PROCESSING',
      },
      _sum: { amount: true },
    });

    const totalCompleted = Number(completedPayouts._sum.amount || 0);
    const totalProcessing = Number(processingPayouts._sum.amount || 0);
    const expectedRemaining = Number(quest.budget_total) - totalCompleted - totalProcessing;
    const actualRemaining = Number(quest.budget_remaining);

    // Allow for floating point: only correct if drift > $0.01
    if (Math.abs(expectedRemaining - actualRemaining) > 0.01) {
      await prisma.quest.update({
        where: { id: quest.id },
        data: { budget_remaining: expectedRemaining },
      });

      try {
        await prisma.auditEvent.create({
          data: {
            entity_type: 'quest',
            entity_id: quest.id,
            actor_id: 'reconciler',
            event_type: 'budget_drift_corrected',
            payload: {
              previous: actualRemaining,
              corrected: expectedRemaining,
              completed_payout_sum: totalCompleted,
              processing_payout_sum: totalProcessing,
              budget_total: Number(quest.budget_total),
            },
          },
        });
      } catch {
        // Non-blocking
      }

      console.log(`[reconciler] Corrected budget for quest ${quest.id}: ${actualRemaining} → ${expectedRemaining}`);
      result.budgetDriftCorrected++;
    }
  }

  console.log('[reconciler] Reconciliation complete:', result);
  return result;
}
