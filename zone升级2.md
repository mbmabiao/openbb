You are modifying an existing Python Streamlit + Lightweight Charts support/resistance dashboard.

Current status of the repo:
- There is already a `src/zone_structure.py` file.
- It already contains:
  - `Zone` dataclass with fields: `side`, `lower`, `upper`, `center`, `sources`, `state`
  - `ZoneRegistry` with `zones`
  - `match_zones(candidates, registry)`
  - `update_registry_from_candidates(candidates, registry)`
  - `detect_pending_breakouts(registry)`
  - `confirm_breakout(zone)`
  - `create_flipped_zone(zone)`
  - `process_retest(zone)`
  - `build_structure_output(registry)`
- But the current implementation is only a scaffold:
  - `detect_pending_breakouts()` only returns zones with `state == "candidate"`
  - `confirm_breakout()` only sets `state = "confirmed_breakout"`
  - `create_flipped_zone()` only flips side and copies bounds
  - `process_retest()` only sets `state = "retested"`
  - no full lifecycle, no metadata, no failed breakout logic, no retire logic, no real breakout confirmation rules

Your task:
Upgrade the structure layer into a usable, stateful support/resistance engine, while keeping the refactor incremental and compatible with the existing dashboard.

==================================================
1. GOAL
==================================================

Implement the missing parts of the zone structure system so that support/resistance is no longer just a recalculated static line set, but a persistent structural object system with:
- identity persistence
- state transitions
- breakout watch
- resistance-to-support flipping
- retest confirmation / failure
- structure-aware visual output

Do NOT rewrite the whole dashboard.
Do NOT break the existing candidate zone engine.
Do NOT remove existing VP / AVWAP logic.
Add the missing structure logic in a clean, modular way.

==================================================
2. EXISTING DESIGN THAT MUST BE PRESERVED
==================================================

Preserve these layers:
- raw OHLCV data loading
- replay mode
- candidate zone generation from VP / AVWAP
- merge logic
- existing ranking/scoring layer
- existing Streamlit and Lightweight Charts shell

The new structure layer should sit between:
candidate zone generation
and
final ranking / chart rendering

==================================================
3. MISSING FUNCTIONALITY TO IMPLEMENT
==================================================

Implement the following missing features in `src/zone_structure.py` and integrate them into the dashboard flow.

------------------------------------------
3.1 Extend the Zone dataclass
------------------------------------------

Replace the minimal Zone object with a richer structure.

Required fields:
- zone_id: str
- side: str                       # "support" | "resistance"
- lower: float
- upper: float
- center: float
- sources: list[str]
- state: str

New lifecycle / tracking fields:
- first_seen_bar: int = -1
- last_seen_bar: int = -1
- created_at_bar: int = -1
- broken_bar: int | None = None
- retired_bar: int | None = None

New structure metadata:
- parent_zone_id: str | None = None
- touch_count: int = 0
- retest_count: int = 0
- acceptance_bars: int = 0

New scoring / diagnostics fields:
- strength: float = 0.0
- metadata: dict = field(default_factory=dict)

Keep backward compatibility where reasonable.

------------------------------------------
3.2 Implement a real zone identity system
------------------------------------------

Current `match_zones()` only matches by same side + interval overlap.
Upgrade it.

A candidate should match an existing zone when:
- same side
- overlap ratio is sufficient OR center distance is sufficiently small
- width is not wildly different

Implement helper functions:
- zone_width(zone)
- zone_center(zone)
- compute_overlap_ratio(candidate, zone)
- match_score(candidate, zone)

Then update `match_zones()` to choose the best existing match, not just the first one.

------------------------------------------
3.3 Implement registry evolution
------------------------------------------

Current `update_registry_from_candidates()` only appends unmatched zones.

Upgrade it so that:
- matched zones update existing object fields
- lower/upper/center are updated gradually, not hard replaced if possible
- last_seen_bar is updated
- first_seen_bar is preserved
- strength / metadata can be refreshed if provided
- unmatched candidates create new Zone objects
- old zones not seen for too long can be retired

Add retire logic:
- if a zone has not been matched for `zone_retire_bars`, mark it as `retired`

Do not physically delete retired zones immediately.

------------------------------------------
3.4 Implement a real state machine
------------------------------------------

Supported states must include:

Native lifecycle:
- candidate
- confirmed
- pending_breakout

Breakout / flip lifecycle:
- broken_pending_flip
- confirmed_flip
- failed_breakout

Terminal lifecycle:
- retired

Implement explicit transition rules.

Examples:
- candidate -> confirmed
  when touch_count or persistence threshold is met

- confirmed -> pending_breakout
  when price approaches the boundary closely enough

- pending_breakout -> broken_pending_flip
  when breakout confirmation rules are satisfied

- broken_pending_flip -> confirmed_flip
  when retest succeeds and acceptance bars pass

- broken_pending_flip -> failed_breakout
  when price falls back through the flipped zone and stays there

- any active state -> retired
  when stale / invalidated / no longer relevant

------------------------------------------
3.5 Implement real pending breakout detection
------------------------------------------

Current `detect_pending_breakouts()` is placeholder logic.

Replace it with actual logic:
- inspect confirmed resistance zones
- detect when close is within `near_breakout_pct` of zone upper
- optionally inspect support for downside breakdown symmetry
- set state to `pending_breakout`
- return the corresponding list

This should not simply return all candidates.

Add fields to metadata if useful:
- pending_since_bar
- breakout_watch_expiry_bar

------------------------------------------
3.6 Implement real breakout confirmation
------------------------------------------

Current `confirm_breakout()` only changes state.

Replace it with a function that checks actual breakout conditions against price bars.

For upward breakout of resistance:
- high must touch or exceed upper
- close must be above upper * (1 + breakout_buffer_pct)
- optionally require consecutive closes outside zone
- record broken_bar
- transition original zone appropriately

Return both:
- whether breakout is confirmed
- updated zone
- optionally a breakout event object or metadata

------------------------------------------
3.7 Implement flipped zone creation properly
------------------------------------------

Current `create_flipped_zone()` only flips side and copies the old zone.

Replace it with full flip creation logic.

When a resistance breakout is confirmed:
- create a new support zone
- side = "support"
- state = "broken_pending_flip"
- parent_zone_id = original resistance zone_id
- bounds should be based on original breakout boundary plus retest buffer
- created_at_bar must be set
- sources should be preserved and include `"flipped"`

Similarly, allow the reverse for support breakdown -> flipped resistance if desired, but priority is upside resistance-to-support.

------------------------------------------
3.8 Implement retest processing
------------------------------------------

Current `process_retest()` only mutates state.

Replace it with actual retest evaluation logic.

For a flipped support zone:
- detect when future bars retest the support region
- count retests
- confirm support if:
  - low probes the region
  - close holds above lower support threshold
  - acceptance bars accumulate above the zone
- mark `confirmed_flip` when successful

Failure logic:
- if price closes back below the flipped support with enough persistence
- mark `failed_breakout`

Track:
- retest_count
- acceptance_bars

------------------------------------------
3.9 Implement structure output
------------------------------------------

Current `build_structure_output()` returns a flat list.

Replace it with a richer structure output object or dict, for example:
- active_support_zones
- active_resistance_zones
- pending_breakout_zones
- flipped_support_zones
- failed_breakout_zones
- retired_zones (optional)

Each output record should include:
- zone_id
- side
- lower
- upper
- center
- sources
- state
- parent_zone_id
- touch_count
- retest_count
- acceptance_bars

==================================================
4. DASHBOARD INTEGRATION REQUIREMENTS
==================================================

Do not rewrite the whole Streamlit app.
Integrate the new structure layer incrementally.

Refactor the dashboard flow so that:
1. candidate zones are generated as before
2. candidates are passed into the structure manager
3. structure output is produced
4. chart rendering consumes structure output, not just raw ranked zones

Add the structure layer after candidate merging and before final rendering.

Keep existing ranking functions where possible.
You may adapt ranking to prefer:
- confirmed_flip zones over plain support zones
- confirmed / pending_breakout zones over raw candidates

==================================================
5. SIMPLE VISUALISATION UPGRADE PLAN
==================================================

Also implement a simple visualisation improvement plan.

Do not build a huge new UI.
Keep it simple and compatible with current Lightweight Charts rendering.

Required visual enhancements:
- Native resistance: red
- Native support: green
- Pending breakout: yellow/orange
- Broken pending flip: orange
- Confirmed flip: teal / blue-green
- Failed breakout: gray-red
- Retired: optionally hidden or faded gray

Visual rules:
1. Continue to draw the current horizontal level representation.
2. Add state-aware label text, e.g.
   - R1 [confirmed]
   - PB1 [pending_breakout]
   - F1 [confirmed_flip]
   - FB1 [failed_breakout]
3. Left-side Streamlit panel should gain sections for:
   - Resistance
   - Pending Breakout
   - Support
   - Flipped Support
4. The tables should include the new structural fields:
   - zone_id
   - state
   - parent_zone_id
   - first_seen_bar
   - last_seen_bar
   - retest_count
   - acceptance_bars
5. Keep the UI minimal and readable.

If shaded boxes are too invasive for now, keep horizontal lines but make line color and label depend on state.

==================================================
6. IMPLEMENTATION STYLE
==================================================

Requirements:
- clean Python
- small, testable functions
- type hints
- no giant monolithic function
- preserve replay compatibility
- preserve existing candidate zone engine
- preserve Streamlit usability

Prefer incremental refactor over rewrite.

==================================================
7. DELIVERABLES
==================================================

Deliver all of the following:

1. Updated `src/zone_structure.py`
2. Any small helper functions needed
3. Necessary integration changes in the dashboard page
4. Updated chart rendering so state-aware zones can be drawn
5. Updated left panel / tables with the new structural fields
6. Short code comments explaining the transition logic

==================================================
8. IMPORTANT CONSTRAINTS
==================================================

- Do not remove existing VP / AVWAP candidate logic
- Do not remove replay mode
- Do not break current chart rendering
- Do not over-engineer the first pass
- Make the first version functional, readable, and easy to extend

==================================================
9. OUTPUT FORMAT
==================================================

Please provide:
1. a short implementation plan
2. the updated code
3. a short note describing what is now fully implemented vs still intentionally simplified