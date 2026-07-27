# NRFI Model — Rebuild Analysis Log

## 2026-07-26 — First validated block-level analysis (slate_log, n=202, 17 days)

Data: `data/slate_log.json`, 202 resolved games (2026-07-08 → 07-26), base rate 55.0% NRFI.
This is the first cycle with per-game component logging, so the first time block
weights could be validated against outcomes rather than reasoned about.

### Headline: the revamp works
- Model discrimination `corr(nrfi_prob, outcome)` = **+0.160** (old April model was +0.049).
- Terciles: LOW 44.8% → MID 53.7% → **HIGH 66.2%** (top decile 70%).
  The top third is squarely in the 60–65% target for selective bets.

### Block signal (standardized multivariate logistic coefficients)
| block | coef | verdict |
|-------|------|---------|
| pitcher | +0.274 | robust — positive in both sub-periods (+0.238, +0.068) |
| park_weather | +0.205 | **unstable** — sign flips by sub-period (−0.079 → +0.265); not trustworthy on 17 days |
| lineup | −0.074 | near-zero |
| batter | +0.006 | **no signal** — flat across terciles (weak-lineup 51.5% vs strong 55.2%) |
| damage_speed | dead | constant 0.500, std=0.0000 (see below) |

### Decision: DO NOT reweight (this cycle)
Out-of-sample test (derive on first ~9 days, test on last ~8) — top-tercile hit rate
on held-out games:
- **OLD/current live weights: 0.722** ← best
- cut-batter: 0.639 · pitcher-heavy: 0.639 · pitcher+park: 0.639 · pitcher-pure: 0.583

Every reweighting scheme did **worse** out-of-sample. 17 days / 202 games is too small;
the block correlations have ~0.07 SE, and weights fit to this window overfit and
degrade the betting-relevant top end. The current revamp weights already generalize
best. Revisit block weights after ≥6 weeks of data.

### damage_speed is a dead constant (0.500 every game)
Its inputs (Savant batted-ball hard_hit/barrel, sprint speed, pop time) aren't
populating in production → everything defaults to league average → exactly 0.5.
Because it's constant it has **zero effect on ranking** (not a regression), but a
working version could add a real signal block. Could not diagnose/fix offline:
the MLB + Savant APIs are blocked in the analysis sandbox by egress policy.
→ This cycle adds diagnostics (`ds_detail`: n_hh / n_spd / n_pop match counts) so the
next cycle can see *which* feed is empty from the logged data alone.

### Instrumentation added this cycle (for the next rebuild)
- `pit_detail.components` — the 8 pitcher sub-scores per half → enables validated
  **P_WEIGHTS** tuning (block log couldn't touch sub-weights).
- `ds_detail` — damage/speed values + feed match-counts → remote diagnosis of the
  dead block.
- Diagnostic detail is written to `slate_log.json` only, stripped from public `index.html`.

### Next cycle checklist (~3–4 more weeks of data)
1. Re-run the block out-of-sample test with the larger sample; only reweight if a
   scheme beats current live weights on held-out top-tercile.
2. Regress `pit_detail.components` → outcome to tune P_WEIGHTS.
3. Read `ds_detail` counts: if n_spd/n_pop are 0 league-wide, fix the sprint/pop
   Savant field names; if n_hh is 0, fix the Savant batter batted-ball parse.
