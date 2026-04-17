# Privacy Guarantee — PayPerQuery Data Marketplace

**Version:** 2.0
**Date:** 2026-04-16
**Status:** For whitepaper inclusion

---

## Threat Model

PayPerQuery is a data marketplace where contributors submit receipt images in
exchange for micropayments. The system stores contributor-provided data
(wallet addresses, ZIP prefixes, justification text, receipt images) and
computes fraud scores using deterministic, rule-based heuristics.

The primary privacy adversaries are:

1. **External attacker** with network access to the API.
2. **Curious buyer** who submits quests to extract contributor data beyond
   what the predicate answers reveal.
3. **Compromised database** — attacker with read access to PostgreSQL.
4. **Compromised filesystem** — attacker with read access to the application
   server.

We do **not** claim privacy against a fully compromised operator (someone with
both database and application access). Operator trust is assumed.

---

## What We Guarantee (and the basis for each claim)

### G1. Predicate-Answer Privacy: Contributors Do Not Leak Raw Field Values

**Claim:** A buyer who submits a quest learns only whether each contributor's
receipt satisfies the buyer's eligibility predicates (pass/fail). The buyer
does not learn the raw OCR-extracted values (exact amount, exact date, exact
merchant string).

**Basis:** The public status endpoint (`GET /submissions/:id/status`) returns
only a categorical status and a generic rejection reason from a fixed set of
three templates. The full verification trace — including observed field values,
predicate comparisons, and exact thresholds — is restricted to admin-only
endpoints behind API key authentication. Buyers do not receive admin API keys.

**Residual risk:** A buyer who creates multiple quests with narrowing
thresholds can triangulate raw values. See §Limitations.

### G2. No Learned Representation of Contributor Behavior

**Claim:** The fraud scoring pipeline produces no model artifact, embedding, or
statistical summary that encodes patterns from individual contributors.

**Basis:** Fraud detection is entirely rule-based: SHA-256 fingerprint
deduplication, velocity counting via SQL `COUNT(*)`, and keyword matching.
There are no trainable parameters. Membership inference, model inversion, and
attribute inference attacks against a trained model are inapplicable — there is
no model.

**Scope:** This guarantee applies only to the fraud scoring layer. It does not
extend to any future ML components. If a trainable model is introduced, it must
be trained with (ε,δ)-differential privacy guarantees (recommended: ε ≤ 1.0,
δ ≤ 1/n² for dataset size n) before this claim can be re-asserted.

### G3. Fingerprint Irreversibility

**Claim:** Receipt fingerprints (`SHA-256(merchant|date|amountCents)`) and
device fingerprints (`SHA-256(user_agent|ip_prefix)`) cannot be reversed to
recover original inputs under standard cryptographic assumptions.

**Basis:** SHA-256 preimage resistance (256-bit security).

**Caveat:** Receipt fingerprints operate over a structured, moderate-entropy
domain. For a fixed merchant list (e.g., 10 merchants), 365 days, and
amounts $0.01–$999.99, the input space is ~3.6 × 10⁹ — feasibly enumerable.
An attacker with the fingerprint and knowledge of the domain could brute-force
the original values in seconds. **This is not a privacy mechanism; it is a
deduplication mechanism.** Do not represent fingerprint hashing as a privacy
guarantee against an attacker who knows the input domain.

### G4. Device Fingerprints Do Not Encode Wallet Identity

**Claim:** Device fingerprints are wallet-independent, so the fraud scoring
layer cannot be used to link two submissions to the same wallet based solely
on device fingerprint comparison.

**Basis:** Device fingerprint is `SHA-256(user_agent|ip_prefix)` — wallet is
not an input. Two wallets from the same device share a fingerprint (enabling
Sybil detection) but the fingerprint cannot be used to determine which wallet
submitted which record.

---

## What We Do NOT Guarantee (Honest Limitations)

### L1. Database-Layer Privacy: Not Provided

Submission records are stored **in plaintext** in PostgreSQL:
- `wallet` (pseudonymous but linkable on-chain)
- `zip_prefix` (geographic identifier)
- `justification_text` (free-text, potentially contains PII)
- `receipt_url` (path to unencrypted image on disk)
- `device_fingerprint` (persistent cross-session tracking identifier)

An attacker with database read access can reconstruct a complete behavioral
profile per wallet: what they bought, when, where, how much, and from which
device. **This is the dominant privacy risk in the system**, not model
inversion.

Mitigations not yet implemented:
- Column-level encryption for `justification_text` and `zip_prefix`
- Receipt image encryption at rest
- Automatic PII expiry / data retention limits
- Wallet address rotation or blinding

### L2. Cross-Quest Linkability: Full

A contributor's wallet appears in every submission they make across all quests.
Any party with access to multiple quest results (the platform operator, or a
buyer who creates multiple quests) can link a contributor's full submission
history. The system provides **no unlinkability across quests**.

### L3. Predicate Triangulation: Partially Mitigatable

A buyer who creates quests with `amount <= $50`, `amount <= $30`, and
`amount <= $20` can infer that a contributor's receipt amount falls in a
specific band based on which quests they pass. With sufficiently many quests,
the buyer reconstructs the raw value to arbitrary precision.

Current mitigation: detection queries flag buyers with narrowing threshold
patterns. Structural mitigation (not yet implemented): quantize predicate
thresholds to coarse bands, charge per predicate configuration.

### L4. Timing Side Channels: Not Addressed

Submission processing time depends on OCR latency, queue depth, and fraud
check complexity. An adversary observing response timing may infer whether
the fraud guard performed additional duplicate checks (implying prior
submissions exist). We do not claim constant-time evaluation.

### L5. Receipt Images: Stored Unencrypted

Receipt images at `uploads/raw/` are accessible to anyone with filesystem
access. These may contain PII beyond what OCR extracts (customer name,
payment method last-4, loyalty program ID).

---

## Comparison to Differential Privacy

Differential privacy is designed to bound information leakage from the
**output of a computation over a dataset** — typically a trained model or
aggregate statistic. In PayPerQuery's current architecture:

- There is no trained model → DP-SGD is inapplicable.
- Predicate answers (pass/fail per submission) are **exact**, not noisy →
  applying DP to predicate evaluation would require injecting randomized
  response noise (flipping pass→fail with probability p), which directly
  conflicts with the system's correctness guarantee.
- The dominant privacy risk is plaintext storage, not computation output →
  DP does not help.

**If DP is desired for predicate answers** (e.g., to prevent triangulation),
the appropriate mechanism is **local differential privacy with randomized
response**: each predicate answer is flipped with probability p = 1/(1+e^ε).
At ε=1.0, ~27% of answers are flipped, introducing significant noise. This
is a product decision with direct accuracy impact — it means ~27% of approved
submissions are false positives and ~27% of rejections are false negatives.

---

## Summary for Peer Review

| Threat | Claimed Protection | Basis | Honest Assessment |
|---|---|---|---|
| Model inversion / membership inference | Protected | No model exists | Trivially true, not interesting |
| API-layer predicate oracle | Protected (v2.0) | Trace redaction | Effective against naive probing |
| Buyer triangulation via multiple quests | Partially detected | Detection queries | Not structurally prevented |
| Database exfiltration | **NOT protected** | Plaintext storage | Dominant real-world risk |
| Filesystem access to receipts | **NOT protected** | Unencrypted images | High severity |
| Cross-quest behavioral linkability | **NOT protected** | Shared wallet identifiers | Inherent to architecture |
| Timing side channels | **NOT protected** | Variable processing time | Low severity |

A cryptographer reviewing this document should conclude: the system makes
narrow, defensible claims about the scoring layer and is transparent about
the significant gaps at the storage and linkability layers. The honest
limitations section is more important than the guarantees section.
