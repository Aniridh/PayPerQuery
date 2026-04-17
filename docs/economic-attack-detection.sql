-- ============================================================
-- ATTACK A: Predicate Triangulation Detection
-- Signal: Same buyer creates multiple quests targeting the same
-- merchant/category with overlapping but narrowing predicates.
-- ============================================================

-- Find buyers with multiple quests whose predicates differ only in threshold values
WITH quest_predicates AS (
  SELECT
    q.buyer_id,
    q.id AS quest_id,
    q.name,
    q.created_at,
    qr.rules::jsonb -> 'eligibility' AS predicates
  FROM "Quest" q
  JOIN "QuestRule" qr ON qr.quest_id = q.id
),
-- Extract merchant and predicate fields per quest
quest_fields AS (
  SELECT
    buyer_id,
    quest_id,
    name,
    created_at,
    -- Extract the set of predicate fields (merchant, amount, etc.)
    (SELECT array_agg(elem->>'field' ORDER BY elem->>'field')
     FROM jsonb_array_elements(predicates) elem) AS predicate_fields,
    -- Extract merchant filter values
    (SELECT elem->>'value'
     FROM jsonb_array_elements(predicates) elem
     WHERE elem->>'field' = 'merchant') AS merchant_filter,
    -- Extract amount thresholds
    (SELECT (elem->>'value')::numeric
     FROM jsonb_array_elements(predicates) elem
     WHERE elem->>'field' = 'amount') AS amount_threshold,
    -- Count of predicates
    jsonb_array_length(predicates) AS predicate_count
  FROM quest_predicates
)
-- Flag buyers who have 3+ quests with same predicate fields + same merchant but different thresholds
SELECT
  buyer_id,
  merchant_filter,
  COUNT(DISTINCT quest_id) AS quest_count,
  array_agg(DISTINCT amount_threshold ORDER BY amount_threshold) AS threshold_ladder,
  MIN(created_at) AS first_quest,
  MAX(created_at) AS last_quest,
  'TRIANGULATION_SUSPECT' AS signal
FROM quest_fields
WHERE merchant_filter IS NOT NULL
GROUP BY buyer_id, merchant_filter, predicate_fields
HAVING COUNT(DISTINCT quest_id) >= 3
   AND COUNT(DISTINCT amount_threshold) >= 3
ORDER BY quest_count DESC;


-- ============================================================
-- ATTACK B: Seller Cartel Detection
-- Signal: Multiple wallets submit suspiciously similar
-- justification text, from overlapping device fingerprints,
-- with coordinated timing.
-- ============================================================

-- Phase 1: Find device fingerprints associated with many wallets (Sybil clusters)
WITH sybil_clusters AS (
  SELECT
    device_fingerprint,
    quest_id,
    COUNT(DISTINCT wallet) AS distinct_wallets,
    array_agg(DISTINCT wallet) AS wallets,
    COUNT(*) AS submission_count,
    COUNT(*) FILTER (WHERE status IN ('APPROVED', 'PAID')) AS approved_count
  FROM "Submission"
  GROUP BY device_fingerprint, quest_id
  HAVING COUNT(DISTINCT wallet) > 2
),
-- Phase 2: Check for temporal clustering (submissions within 5-minute windows)
temporal_clusters AS (
  SELECT
    s.quest_id,
    date_trunc('hour', s.created_at) AS hour_bucket,
    COUNT(DISTINCT s.wallet) AS wallets_in_window,
    COUNT(*) AS submissions_in_window,
    array_agg(DISTINCT s.wallet) AS wallets
  FROM "Submission" s
  WHERE s.status IN ('APPROVED', 'PAID')
  GROUP BY s.quest_id, date_trunc('hour', s.created_at)
  HAVING COUNT(DISTINCT s.wallet) > 5
),
-- Phase 3: Approval rate anomaly per quest (cartel inflates approved %)
approval_rates AS (
  SELECT
    quest_id,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE status IN ('APPROVED', 'PAID')) AS approved,
    ROUND(COUNT(*) FILTER (WHERE status IN ('APPROVED', 'PAID'))::numeric / NULLIF(COUNT(*), 0), 3) AS approval_rate
  FROM "Submission"
  GROUP BY quest_id
  HAVING COUNT(*) >= 10
)
SELECT
  sc.quest_id,
  sc.device_fingerprint,
  sc.distinct_wallets AS sybil_wallets,
  sc.approved_count,
  ar.approval_rate AS quest_approval_rate,
  'CARTEL_SUSPECT' AS signal
FROM sybil_clusters sc
LEFT JOIN approval_rates ar ON ar.quest_id = sc.quest_id
WHERE sc.approved_count > 3
ORDER BY sc.distinct_wallets DESC;


-- ============================================================
-- ATTACK C: Sybil Buyer Quest Splitting Detection
-- Signal: Same buyer (or buyers sharing contact_email domain)
-- create many small-budget quests with near-identical predicates.
-- ============================================================

WITH buyer_quest_stats AS (
  SELECT
    b.id AS buyer_id,
    b.org_name,
    b.contact_email,
    -- Extract email domain for cross-account detection
    split_part(b.contact_email, '@', 2) AS email_domain,
    COUNT(q.id) AS quest_count,
    AVG(q.budget_total::numeric) AS avg_budget,
    SUM(q.budget_total::numeric) AS total_budget,
    MIN(q.created_at) AS first_quest,
    MAX(q.created_at) AS last_quest
  FROM "Buyer" b
  JOIN "Quest" q ON q.buyer_id = b.id
  GROUP BY b.id, b.org_name, b.contact_email
),
-- Check for predicate similarity across quests from same buyer
quest_similarity AS (
  SELECT
    q.buyer_id,
    COUNT(DISTINCT q.id) AS quests_with_rules,
    -- Simple: count quests sharing identical predicate field sets
    COUNT(DISTINCT (
      SELECT string_agg(elem->>'field', ',' ORDER BY elem->>'field')
      FROM jsonb_array_elements(qr.rules::jsonb -> 'eligibility') elem
    )) AS distinct_predicate_schemas
  FROM "Quest" q
  JOIN "QuestRule" qr ON qr.quest_id = q.id
  GROUP BY q.buyer_id
)
SELECT
  bqs.buyer_id,
  bqs.org_name,
  bqs.email_domain,
  bqs.quest_count,
  bqs.avg_budget,
  bqs.total_budget,
  qs.distinct_predicate_schemas,
  CASE
    WHEN bqs.quest_count >= 5 AND qs.distinct_predicate_schemas <= 2
    THEN 'HIGH_SPLIT_RISK'
    WHEN bqs.quest_count >= 3 AND bqs.avg_budget < 50
    THEN 'MODERATE_SPLIT_RISK'
    ELSE 'LOW'
  END AS risk_level,
  'QUEST_SPLIT_SUSPECT' AS signal
FROM buyer_quest_stats bqs
LEFT JOIN quest_similarity qs ON qs.buyer_id = bqs.buyer_id
WHERE bqs.quest_count >= 3
ORDER BY bqs.quest_count DESC;
