# Analytics Test Specs

This folder documents *how* analytics metrics should be validated.
It is intentionally light-weight (not pytest) so it can serve as a
human-readable contract for new metrics and QA reviews.

## What makes a **good** metric
- **Stable definition**: formula and inputs are explicit and versioned.
- **Well-scoped**: depends on a clear input universe and time bucket.
- **Robust**: handles missing inputs without silent shape changes.
- **Consistent units**: unit is explicit (e.g., EUR, bps, count, ratio).
- **Reproducible**: same input yields same output (deterministic).

## What makes a **bad** metric
- **Ambiguous**: unclear input selection or filtering rules.
- **Unbounded**: no sanity range or unit, so values are hard to interpret.
- **Unstable**: output changes when unrelated options change.
- **Opaque**: no doc or mapping to implementation.

## Suggested tests for new metrics
1. **Null-rate check**
   - Expect null rate < 5% on a known-good fixture.
   - Failing threshold: null rate > 20% on the same fixture.

2. **Range check**
   - Define realistic bounds, e.g. spreads in [0, 1e5] bps.
   - Flag values outside bounds as a failure.

3. **Idempotency check**
   - Run twice on the same input; output should be identical.

4. **Sensitivity check**
   - Small input perturbation should not cause discontinuous spikes.

5. **Unit check**
   - Unit label in schema must match the expected scale.

## Example acceptance criteria
- Good metric: `TradeTotalVolume` sums to the expected total per bucket.
- Bad metric: `SpreadBps` contains negative values or > 1e6 bps.
