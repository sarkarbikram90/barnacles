# Barnacles UI — Production-Grade Observability Dashboard Upgrade

You are a senior product designer, frontend engineer, and observability-platform engineer.

The Barnacles backend and core UI already exist and are functional. The current dashboard is visually strong and should be treated as the foundation rather than redesigned from scratch.

Your task is to upgrade the existing Barnacles dashboard from approximately **8.5/10 to 9.5/10** in visual quality, information architecture, usability, operational clarity, consistency, and production readiness.

The goal is to make Barnacles feel like a credible modern observability/log-management product while preserving its existing identity.

Do not turn it into a generic admin dashboard.

Do not blindly add features.

Do not add visual clutter.

Prioritize:

* operator usability
* information hierarchy
* visual consistency
* semantic correctness
* scanning speed
* failure visibility
* responsive behavior
* accessibility
* performance
* maintainability

---

# 1. Existing UI

The current UI has:

* dark operational theme
* Barnacles branding
* top-level status indicators
* log table
* search
* severity filtering
* host filtering
* source filtering
* pause
* auto-scroll
* historical fetch
* clear
* log details/view action
* real-time log streaming

The current visual language uses:

* dark navy/black background
* subtle borders
* cyan/blue accents
* compact typography
* glowing operational indicators
* dense tabular layout

Preserve this visual identity.

Do not replace the design language with another design system unless required by the existing implementation.

---

# 2. First Step: Audit Before Changing Anything

Before implementing changes, perform a complete UI audit.

Do not immediately start coding.

Inspect:

* every component
* every CSS rule
* every layout container
* all typography
* color usage
* spacing
* border radius
* shadows
* button styles
* form controls
* badges
* icons
* tables
* modal/drawer behavior
* responsive behavior
* empty states
* loading states
* error states
* WebSocket connection state
* hover states
* focus states
* disabled states
* active states
* scrollbar behavior
* overflow behavior
* long text behavior
* timestamps
* number formatting
* status labels
* severity semantics

Look specifically for inconsistencies such as:

* different border radii for visually equivalent controls
* inconsistent padding
* inconsistent icon sizes
* inconsistent font sizes
* inconsistent capitalization
* inconsistent button heights
* different treatment of similar statuses
* inconsistent badge styling
* inconsistent color semantics
* inconsistent spacing between controls
* misaligned table headers and cells
* inconsistent hover behavior
* inconsistent focus indicators
* inconsistent empty states
* inconsistent tooltip behavior
* inconsistent terminology
* duplicated visual patterns
* controls that look interactive but are not
* controls that do not look interactive but are
* elements that visually compete for attention unnecessarily

Fix those inconsistencies as part of this work.

Do not merely document them.

---

# 3. Establish a UI Design System

Before implementing additional features, establish a small internal design system.

Do NOT add unnecessary dependencies.

Create reusable primitives/components where appropriate.

At minimum establish consistent tokens for:

```text
background
surface
surface-elevated
border
border-subtle
text-primary
text-secondary
text-muted
accent
success
warning
error
info
focus
```

Also define:

```text
spacing-xs
spacing-sm
spacing-md
spacing-lg
spacing-xl
```

and consistent:

```text
radius-sm
radius-md
radius-lg
```

and:

```text
font-size-xs
font-size-sm
font-size-md
font-size-lg
font-size-xl
```

Do not scatter arbitrary values throughout the stylesheet.

Use CSS variables or the project's existing token mechanism.

The goal is consistency, not abstraction for abstraction's sake.

---

# 4. Visual Language

Preserve the current dark observability aesthetic.

The visual tone should communicate:

```text
technical
calm
precise
operational
high-signal
modern
serious
```

Avoid:

* excessive gradients
* excessive neon
* excessive glow
* excessive rounded cards
* giant headings
* oversized whitespace
* colorful dashboards
* decorative charts that do not communicate operational information

The UI should look like something an SRE or platform engineer could stare at for eight hours without wanting to throw the monitor out the window.

---

# 5. Fix Severity Semantics

The current UI displays HTTP status codes such as:

```text
200
201
204
400
404
500
```

alongside:

```text
INFO
```

This creates semantic ambiguity because HTTP status codes are not log severity levels.

Fix the visual model.

The primary `LEVEL` field should consistently use:

```text
DEBUG
INFO
WARN
ERROR
FATAL
```

HTTP status should be represented separately.

For example:

```text
LEVEL     STATUS
INFO      200
INFO      201
WARN      400
ERROR     500
```

Or expose status as structured metadata/details.

Do not make `200`, `400`, or `500` visually resemble severity labels.

---

# 6. Severity Visual Semantics

Define a consistent severity system.

Recommended semantics:

```text
DEBUG   muted/low emphasis
INFO    cyan/blue
WARN    amber
ERROR   red
FATAL   strong red/high emphasis
```

The exact colors must remain compatible with the existing dark theme.

Do not over-saturate the interface.

Severity should be identifiable at a glance without overwhelming the screen.

The same severity colors must be used consistently in:

* table rows
* badges
* details panel
* charts
* counts
* filters
* future alerts

Do not have one shade of red in the table and another unrelated red in the details panel.

---

# 7. Top-Level Operational Summary

Upgrade the top header metrics.

Current:

```text
STATUS
AGENTS
RATE
TOTAL LOGS
```

Keep this structure but make the metrics more operationally useful.

Target:

```text
STATUS
CONNECTED

AGENTS
12 healthy

RATE
421 evt/s

TOTAL LOGS
1.82M
```

Add secondary contextual information where useful:

```text
+18% vs previous interval
```

or:

```text
last 60 seconds
```

Do not overload the cards.

Each metric must have:

* metric name
* primary value
* optional context
* appropriate state indicator

Make units consistent:

```text
evt/s
K
M
GB
ms
%
```

Do not mix formatting styles.

---

# 8. Add Operational KPI Strip

Introduce a compact secondary summary area immediately below the main header or above the filter bar.

Recommended metrics:

```text
EVENTS/SEC
ERROR RATE
ACTIVE AGENTS
INGEST LATENCY
```

Example:

```text
EVENTS/SEC       ERROR RATE       AGENTS          INGEST LATENCY
421 evt/s        1.8%             12 healthy      p95 61 ms
+18%             +0.4%            0 unhealthy     ↓ 12 ms
```

Keep this visually compact.

The KPI strip must not consume excessive vertical space.

Use it to communicate system health, not decoration.

---

# 9. Error Rate

Introduce error-rate visibility.

Define the metric clearly.

For example:

```text
ERROR RATE
1.8%
```

Provide context:

```text
last 5 minutes
```

Optionally compare against the previous interval.

If the backend does not currently expose this metric, add the minimum API/aggregation required.

Do not invent fake values.

If no data exists, display:

```text
—
No data
```

rather than `0`.

---

# 10. Ingest Latency

Add ingestion latency visibility.

Show:

```text
p50
p95
```

where practical.

For example:

```text
INGEST LATENCY
p95 61 ms
```

Do not show meaningless precision such as:

```text
61.293847 ms
```

Use human-readable operational formatting.

---

# 11. Time Range Control

Improve the historical-data experience.

Add a clear time-range selector:

```text
Last 5 min
Last 15 min
Last 1 hour
Last 6 hours
Last 24 hours
Custom
```

Make the selected range visually obvious.

Do not rely on users understanding what “Fetch Historical” means.

Instead provide explicit temporal context.

Example:

```text
TIME RANGE
Last 15 minutes
```

Historical fetch should respect the selected time range.

---

# 12. Filtering System

Improve the filtering architecture.

Current filters:

```text
Search
Level
Host
Source
```

Preserve them but improve consistency.

The filter area should feel like one coherent control system.

Use consistent:

* control heights
* border radius
* padding
* icon treatment
* focus states
* text color
* placeholder style

Add active-filter indicators.

For example:

```text
Level: ERROR
Host: node-01
Source: api
```

Allow easy clearing of individual filters.

Provide:

```text
Clear filters
```

separately from:

```text
Clear logs
```

Do NOT make these actions visually ambiguous.

---

# 13. Search

Make search more powerful but still simple.

Support:

```text
message text
host
source
structured fields
```

Provide a useful placeholder such as:

```text
Search messages, hosts, sources, fields...
```

Do not implement a complicated query language unless the existing backend already supports one.

Search must be debounced if requests are made while typing.

Do not generate unnecessary API requests.

---

# 14. Filter State

Make it obvious when logs are filtered.

Display something like:

```text
Showing 842 of 12,481 events
```

This is extremely useful operationally.

Also display when no results are visible because of filtering.

Do not make an empty filtered result look like a system failure.

---

# 15. Log Table

Preserve the current dense table.

The table should remain the primary workspace.

Recommended columns:

```text
TIMESTAMP
LEVEL
STATUS
HOST
SOURCE
MESSAGE
DETAILS
```

Avoid excessively wide columns.

Timestamp should remain compact.

Message should receive the majority of horizontal space.

Use ellipsis for extremely long messages.

Allow full inspection through the detail view.

---

# 16. Timestamp Formatting

Standardize timestamp display.

Primary table format:

```text
21:44:21.000
```

Optional date/context:

```text
Aug 14
```

Full timestamp belongs in details.

Do not force users to read:

```text
2026-08-14T21:44:21.000Z
```

for every row.

However, preserve the precise timestamp internally.

On hover or details:

```text
2026-08-14T21:44:21.000Z
```

Be explicit about timezone.

---

# 17. Message Rendering

Messages should be:

* highly readable
* left aligned
* visually dominant within the row
* selectable
* safely escaped

Preserve monospaced typography for log message content if consistent with the current design.

Do not allow arbitrary log contents to break the layout.

Handle:

* long messages
* Unicode
* JSON
* escaped strings
* stack traces
* URLs
* multiline logs

Gracefully.

---

# 18. Structured Log Details

Significantly improve the existing `View` behavior.

Clicking `View` should open a side drawer or modal.

Prefer a side drawer if the existing layout supports it.

The drawer should display:

```text
EVENT DETAILS
```

with sections:

```text
Overview
Message
Structured Fields
Source Information
Delivery Metadata
```

Example:

```text
Timestamp
2026-08-14T21:44:21.000Z

Level
ERROR

Host
node-01

Source
system-access

HTTP Status
500

Message
HTTP 500 /api/v1/users in 334ms
```

Then structured fields:

```text
method       GET
path         /api/v1/users
status       500
duration     334ms
request_id   82fd...
agent_id     agent-01
```

The details view should support copying values.

---

# 19. Detail Drawer UX

The details panel should:

* preserve the current log-table context
* be dismissible with Escape
* support click-outside dismissal if appropriate
* have an explicit close control
* trap keyboard focus if implemented as a modal
* remain usable on smaller screens
* handle extremely large messages gracefully
* support copying log content
* preserve whitespace for stack traces

Do not cause the entire page to reflow unnecessarily.

---

# 20. JSON / Structured Data Viewer

If the log includes structured fields, render them clearly.

Do not simply dump a giant JSON string.

Provide a readable key/value representation.

For deeply nested structures, use a collapsible JSON viewer if needed.

Keep the implementation lightweight.

---

# 21. Connection State

Improve WebSocket status semantics.

Supported states:

```text
Connected
Connecting
Disconnected
Reconnecting
Degraded
```

Do not use “Connected” merely because the WebSocket opened once.

Connection state should represent the actual current state.

Provide appropriate visual states.

---

# 22. WebSocket Reconnection UX

Automatically reconnect using exponential backoff.

Show:

```text
Reconnecting...
Attempt 3
```

where useful.

Avoid aggressive reconnect loops.

Provide a subtle status notification when reconnecting.

Do not spam toast notifications every time a connection drops.

---

# 23. Pause / Auto-Scroll

Separate the meanings.

### Pause

Stops live-log rendering/processing in the UI.

### Auto-scroll

Controls whether the viewport follows new events.

These are not the same feature.

Make that semantic distinction obvious.

If auto-scroll is disabled and new logs arrive, show:

```text
↓ 124 new events
```

Provide a button:

```text
Jump to latest
```

This is a major usability improvement for real-time log interfaces.

---

# 24. Historical Data vs Live Stream

Clearly distinguish:

```text
LIVE
```

and:

```text
HISTORICAL
```

The operator should always know whether the currently visible records are:

* currently streaming
* loaded from history
* a mixture

Do not leave this ambiguous.

---

# 25. Log Stream Position

When the user scrolls away from the bottom:

```text
Auto-scroll
```

should automatically turn off or remain explicitly disabled based on the chosen UX.

Show:

```text
124 new events
```

rather than silently forcing the operator back to the bottom.

This is standard behavior for serious log viewers.

---

# 26. Hosts and Sources

Add lightweight contextual visibility for:

```text
Hosts
Sources
```

Possible approach:

A compact collapsible panel/sidebar displaying:

```text
HOSTS

node-01        42K
node-02        31K
node-03        28K

SOURCES

system-access  61K
nginx          24K
postgres       12K
```

Only implement this if it fits naturally into the existing UI.

Do not sacrifice the main log table.

Clicking a host or source should apply the corresponding filter.

---

# 27. Empty State

Implement a proper empty state.

Examples:

### No logs yet

```text
No log events received

Start an agent or generate traffic to begin streaming logs.
```

### No results

```text
No matching events

Try removing a filter or changing the search term.
```

### Historical data unavailable

```text
No events found in this time range.
```

Do not use the same empty state for all scenarios.

---

# 28. Loading State

Do not show blank screens while loading.

Use subtle skeleton or progress states.

Avoid excessive spinners.

For log data, preserve existing content while fetching additional historical records whenever possible.

---

# 29. Error State

Provide clear operational errors.

Example:

```text
Unable to load historical events

The server returned an error.
Retry
```

Do not expose raw backend stack traces.

Do not silently fail.

---

# 30. Error Rate Visualization

If enough telemetry exists, add a compact visual indicator such as:

```text
Error rate
▁▁▂▁▁▃▆█▆▃
```

or a small time-series chart.

Keep it lightweight.

The chart should answer:

> Are errors increasing right now?

Do not create charts merely because dashboards are expected to have charts.

---

# 31. Event Rate Visualization

Similarly, provide a compact events/sec trend if useful:

```text
Events/sec
▂▃▃▅▆▅▇█▆▅
```

Avoid giant charts that consume the majority of the viewport.

The log table must remain the center of gravity.

---

# 32. Anomaly / Incident Readiness

Do not build AI anomaly detection into this iteration.

However, design the UI so the future architecture can support:

```text
Anomalies
Incidents
Alerts
```

For example, leave room in the navigation or architecture for:

```text
Logs
Agents
Anomalies
Incidents
```

Do not expose empty navigation items unless they are functional.

Never ship “Coming Soon” UI just for visual completeness.

---

# 33. Agents View

If agent data is available from the backend, provide an agent status view.

Show:

```text
Agent ID
Hostname
Version
Status
Last Seen
Events/sec
Spool Size
```

Example:

```text
agent-01   node-01   Healthy    2s ago    421 evt/s
agent-02   node-02   Healthy    1s ago    309 evt/s
agent-03   node-03   Warning    14s ago   0 evt/s
```

The agent status should be actionable.

Clicking an agent should filter logs where useful.

---

# 34. Responsive Design

Audit the application at:

```text
1440px
1280px
1024px
768px
480px
```

The desktop experience remains the priority.

At smaller widths:

* filters should wrap intelligently
* table columns should adapt
* details drawer should become full-screen where appropriate
* controls should remain usable
* metrics should stack
* text should not overlap
* no horizontal page scrolling unless unavoidable

Do not simply shrink everything.

Reflow the layout intentionally.

---

# 35. Accessibility

Implement practical accessibility.

Ensure:

* keyboard navigation
* visible focus states
* semantic buttons
* correct labels
* ARIA only where necessary
* adequate contrast
* dialogs have accessible names
* tables have semantic headers
* interactive elements are actually keyboard reachable

Do not remove focus outlines without replacing them.

---

# 36. Keyboard Shortcuts

Add a small number of useful shortcuts.

Recommended:

```text
/
focus search

Esc
close details / clear transient UI

Space
pause/resume stream

L
jump to latest
```

Do not add dozens of shortcuts.

Provide a small keyboard-shortcuts help mechanism if appropriate.

---

# 37. Tooltips

Use tooltips for ambiguous icons.

Do not use tooltips for obvious text buttons.

Tooltip content should explain the action, not repeat the label unnecessarily.

---

# 38. Button Semantics

Establish clear hierarchy:

### Primary

```text
Fetch Historical
```

### Secondary

```text
Pause
Auto-scroll
```

### Destructive

```text
Clear
```

The destructive action should visually stand apart.

Do not make every button blue.

---

# 39. Clear vs Clear Filters

These actions must be different.

`Clear filters` means:

```text
remove query constraints
```

`Clear logs` means:

```text
clear client-side visible log state
```

Do not allow users to confuse them.

If clearing logs is destructive or surprising, require an appropriate confirmation depending on actual semantics.

Never delete server-side logs accidentally from a UI action intended merely to clear the screen.

---

# 40. Number Formatting

Standardize all numerical display.

Examples:

```text
984
1.2K
84.3K
1.4M
```

Use consistent rounding.

For latency:

```text
12 ms
1.2 s
```

For rates:

```text
421 evt/s
4.2K evt/s
```

Do not mix:

```text
421 events/sec
421 evt/s
421/s
```

throughout the UI.

Choose one convention.

---

# 41. Color Consistency Audit

Perform a complete color audit.

Ensure the same semantic concept always uses the same visual treatment.

For example:

```text
Success → green
Warning → amber
Error → red
Info → cyan/blue
Muted → gray
Primary action → blue
```

Do not accidentally use green for “connected” in one place and cyan in another.

Do not use error red for unrelated destructive-looking decorative elements.

---

# 42. Typography Audit

Standardize:

* font family
* font weights
* heading sizes
* table typography
* metadata typography
* button typography
* placeholder typography

The UI should have a clear typographic hierarchy.

Avoid too many font sizes.

---

# 43. Spacing Audit

Use a consistent spacing scale.

Inspect every major area:

```text
header
metrics
KPI strip
filters
table
drawer
empty states
buttons
```

Remove accidental one-off spacing values.

The UI should feel intentionally aligned.

---

# 44. Icon Audit

Ensure icons are:

* consistent in visual weight
* consistent in size
* aligned vertically
* semantically appropriate

Avoid mixing unrelated icon styles.

Do not use icons purely for decoration where they add noise.

---

# 45. Scrollbar and Overflow Audit

Inspect:

* page scrollbar
* table scrollbar
* details drawer
* long messages
* filters
* dropdown menus
* modal content

Prevent:

* double scrollbars
* hidden content
* clipped dropdowns
* content extending outside cards
* horizontal overflow

The current screenshot already shows a vertical scrollbar in the log table/viewport.

Make its behavior deliberate and consistent.

---

# 46. Table Performance

The dashboard may eventually display thousands of events.

Do not render an unbounded number of DOM nodes.

If necessary, introduce virtualization for the log table.

Only do this if the current architecture needs it.

Do not add virtualization prematurely if the existing event count remains small.

However, structure the code so scaling the table later will not require redesigning the entire application.

---

# 47. Real-Time Rendering Performance

Avoid re-rendering the entire dashboard for every incoming event.

Batch UI updates where appropriate.

For example:

```text
incoming events
    ↓
small buffer
    ↓
render every 50–100ms
```

Do not sacrifice perceived real-time behavior.

The dashboard should feel live without causing CPU spikes.

---

# 48. Prevent UI Memory Leaks

Audit:

* WebSocket listeners
* timers
* intervals
* event listeners
* subscriptions
* observers

Ensure cleanup occurs when components unmount or state changes.

The UI should remain stable during long-running sessions.

Test the dashboard for at least several hours of simulated event streaming.

---

# 49. Long-Running Session Test

Simulate:

```text
1,000 events
10,000 events
100,000 events
1M events
```

where appropriate.

Verify:

* memory remains bounded
* DOM size remains reasonable
* scroll behavior remains stable
* WebSocket remains responsive
* filters remain responsive

Do not allow the browser to become a memory landfill.

---

# 50. Visual Regression Review

After implementation, compare the new UI against the current UI.

Do not regress:

* branding
* dark theme
* existing density
* readability
* log visibility
* status indicators
* existing workflow

The upgrade should feel evolutionary, not like a different product.

---

# 51. Inconsistency Discovery Pass

After all changes are implemented, perform another complete UI review specifically searching for inconsistencies.

Check:

```text
alignment
spacing
colors
typography
buttons
badges
icons
borders
radius
hover
focus
disabled
loading
empty
error
responsive
terminology
units
timestamps
severity
status
filters
dialogs
```

Fix every meaningful inconsistency you find.

Do not stop after fixing the first few.

Treat this as a dedicated quality gate.

---

# 52. UX Terminology Audit

Use consistent terminology throughout the application.

Choose one term and use it everywhere.

For example:

```text
Event
Log Event
Agent
Source
Host
Severity
Rate
```

Do not alternate between:

```text
logs
events
records
entries
```

unless they have deliberately different meanings.

Likewise:

```text
Connected
Disconnected
Reconnecting
```

must be consistent everywhere.

---

# 53. Microcopy

Improve text labels so they are operationally explicit.

Examples:

Instead of:

```text
Clear
```

use:

```text
Clear View
```

if that is what the action actually does.

Instead of:

```text
Fetch Historical
```

consider:

```text
Load History
```

if that better reflects the actual behavior.

Do not change terminology arbitrarily.

Base labels on real behavior.

---

# 54. Dashboard Layout Target

The final desktop layout should roughly follow:

```text
┌─────────────────────────────────────────────────────────────────────┐
│ Barnacles                              Status Agents Rate Total     │
├─────────────────────────────────────────────────────────────────────┤
│ Events/sec   Error Rate   Active Agents   Ingest Latency             │
├─────────────────────────────────────────────────────────────────────┤
│ Search       Level   Host   Source   Time Range   Filters            │
├─────────────────────────────────────────────────────────────────────┤
│ Live / Historical                 842 of 12,481 events              │
├─────────────────────────────────────────────────────────────────────┤
│ Timestamp │ Level │ Status │ Host │ Source │ Message │ Details      │
│                                                                     │
│ 21:44:21  │ INFO  │ 200    │ ...  │ ...    │ ...     │ View         │
│ 21:44:22  │ WARN  │ 400    │ ...  │ ...    │ ...     │ View         │
│ 21:44:23  │ ERROR │ 500    │ ...  │ ...    │ ...     │ View         │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│ ↓ 124 new events                         Jump to latest              │
└─────────────────────────────────────────────────────────────────────┘
```

The exact layout may differ based on the implementation.

The principles are:

* header
* operational KPIs
* coherent filters
* context/status
* dense log table
* clear live-stream behavior

---

# 55. Details Drawer Target

Example:

```text
┌─────────────────────────────────────────┐
│ EVENT DETAILS                        X   │
├─────────────────────────────────────────┤
│ ERROR                                   │
│                                          │
│ Timestamp                                │
│ 2026-08-14T21:44:23.000Z                 │
│                                          │
│ Host             node-01                 │
│ Source           system-access           │
│ HTTP Status      500                     │
│ Duration         334 ms                  │
│ Agent            agent-01                │
│                                          │
│ MESSAGE                                  │
│ HTTP 500 /api/v1/users in 334ms          │
│                                          │
│ STRUCTURED FIELDS                        │
│ method         GET                       │
│ path           /api/v1/users             │
│ request_id     82fd...                   │
│                                          │
│ [Copy Event]                             │
└─────────────────────────────────────────┘
```

Keep it visually aligned with the main dashboard.

---

# 56. Do Not Add These Things

Do not add:

* unnecessary charts
* huge cards
* marketing copy
* giant logos
* excessive animations
* decorative gradients everywhere
* fake AI features
* fake anomaly scores
* fake data
* fake health metrics
* unnecessary pagination
* unnecessary tabs
* unnecessary side navigation
* excessive toast notifications

Every UI element must have a real operational purpose.

---

# 57. Backend Changes

If implementing the UI requires backend changes, make the minimum necessary changes.

Examples:

* error-rate aggregation
* ingest latency metrics
* agent status endpoint
* historical query metadata
* structured field retrieval
* event counts
* time-range queries

Do not create a second backend architecture solely for the dashboard.

Keep API contracts clean.

Version APIs when required.

---

# 58. API Contract Quality

The frontend must not infer critical semantics from presentation strings.

For example, do not determine severity by parsing:

```text
"HTTP 500"
```

The API should provide:

```json
{
  "level": "ERROR",
  "status": 500
}
```

The UI should render those fields independently.

This principle applies to:

* severity
* HTTP status
* timestamp
* host
* source
* agent
* latency
* structured fields

---

# 59. Accessibility and Keyboard QA

Test the application without a mouse.

Verify:

* all controls reachable
* filters usable
* dropdowns usable
* details drawer usable
* close works with Escape
* search focus shortcut works
* focus is visible
* focus does not disappear into the page

Do not ship keyboard-inaccessible controls.

---

# 60. Browser Compatibility

Verify current versions of:

* Chrome
* Edge
* Firefox

Do not rely on browser-specific behavior unnecessarily.

---

# 61. Performance Acceptance Criteria

The dashboard should:

* load quickly
* remain responsive during high event rates
* maintain bounded memory
* avoid excessive DOM growth
* avoid blocking the main thread
* avoid unnecessary network calls
* avoid unnecessary re-renders

Use browser profiling where needed.

Do not optimize based solely on assumptions.

---

# 62. Code Quality

Follow the existing project's coding conventions.

Maintain:

* small components
* clear responsibilities
* predictable state ownership
* explicit data flow
* reusable primitives where justified
* minimal duplication
* readable CSS
* meaningful names

Do not create giant components.

Do not create a generic abstraction framework.

---

# 63. Final QA Checklist

Before declaring the UI complete, verify all of the following.

## Visual

* [ ] consistent spacing
* [ ] consistent typography
* [ ] consistent colors
* [ ] consistent buttons
* [ ] consistent badges
* [ ] consistent borders
* [ ] consistent radius
* [ ] consistent icons
* [ ] no accidental visual noise
* [ ] no alignment problems

## Semantics

* [ ] severity is distinct from HTTP status
* [ ] metrics use consistent units
* [ ] timestamps are consistent
* [ ] status semantics are correct
* [ ] terminology is consistent

## Interaction

* [ ] search works
* [ ] filters work
* [ ] active filters are visible
* [ ] filters can be cleared independently
* [ ] historical queries work
* [ ] pause works
* [ ] auto-scroll works
* [ ] jump-to-latest works
* [ ] reconnect works
* [ ] details drawer works
* [ ] copying works

## Reliability

* [ ] WebSocket disconnect is handled
* [ ] reconnect does not loop aggressively
* [ ] timers are cleaned up
* [ ] listeners are cleaned up
* [ ] long-running sessions do not leak memory
* [ ] large messages do not break layout

## Responsive

* [ ] 1440px
* [ ] 1280px
* [ ] 1024px
* [ ] 768px
* [ ] 480px

## Accessibility

* [ ] keyboard navigation
* [ ] visible focus
* [ ] semantic buttons
* [ ] accessible drawer
* [ ] adequate contrast
* [ ] screen-reader-friendly labels

---

# 64. Final Product Review

After implementation, do not simply report that all requested features were added.

Act as an independent senior product reviewer.

Evaluate the resulting UI on:

```text
Visual hierarchy
Operational usability
Information density
Consistency
Accessibility
Responsiveness
Performance
Observability value
Error visibility
Interaction design
```

Then inspect the UI for anything that still looks:

* unfinished
* inconsistent
* generic
* visually noisy
* misleading
* duplicated
* overly complicated
* unclear
* non-operational

Fix those issues before completion.

Do not stop at the explicitly requested changes.

---

# 65. Final Success Criteria

The final UI should feel like:

> “A serious internal observability product that could plausibly be used by an SRE/platform engineering team.”

It should NOT feel like:

> “A developer demo with a dark theme.”

The final experience must communicate:

```text
What is happening?
Is the system healthy?
Where are the problems?
How much traffic is flowing?
Which hosts are affected?
Which sources are generating errors?
Can I inspect the event?
Can I move back in time?
Can I safely monitor the live stream?
```

Those answers should be available within seconds of opening the dashboard.

Target:

**9.5/10 visual and operational quality.**

Do not chase perfection through unnecessary complexity.

Make the interface **calm, precise, dense, consistent, fast, and operationally useful.**
