# Design: split the README into what, how, and why

Status: Approved
Branch: `docs/simplify-readme` (off main @ aeae0eb)

## Problem

The README overhaul in `aeae0eb` made the root README a standalone reviewer entry
point. It succeeded at completeness and overshot on length: 358 lines, where roughly
half is rationale prose — why the message key is the eight-field `ReportKey`, why the
startup gate exists, why the API returns `503` rather than an empty `200`, why the CSV
and the JSON diverge, what changes at 100x, why ingestion is REST-triggered.

That reasoning is worth keeping. It is not worth putting between a reviewer and the
three commands that start the stack. Two specific costs:

- **The operational path is buried.** "How do I run this" sits at line 130, behind two
  diagrams and two rationale sections.
- **The detailed mermaid diagram carries too much.** Partition counts, the full
  `ReportKey.encode()` field list, the `transactionId` hash expression and both state
  stores are all inside diagram nodes. It answers questions a reviewer has not asked
  yet, and it is unreadable at a glance.

## Decisions

1. **Three documents, split by the question each answers.**

   | Doc | Question |
   |---|---|
   | `README.md` | What is this, how do I run it, where is each requirement met |
   | `docs/architecture.md` | How is it put together |
   | `docs/design-notes.md` | Why is it like that |

   The 9 per-slice specs in `docs/superpowers/specs/` remain the archive of full depth.
   `design-notes.md` indexes them; it does not replace them.

2. **README target is ~130 lines.** Sections kept: Problem, Architecture (simple
   diagram + module table), Quick start, Using your own file, Assumptions, API
   endpoints, Requirements traceability, Testing, Tech stack, Known limitations,
   Further reading. Sections removed outright: Scalability, "CSV vs JSON: a deliberate
   divergence", the "Assumptions and design rationale" prose, and the "Design
   decisions" table — all relocated, none deleted.

3. **The README diagram is one simple mermaid, replacing both current diagrams.**
   Six nodes, left to right, no annotations: `Input.txt → ingestion-service → Kafka →
   processing-service → REST API → Angular UI`. No partition counts, no store names,
   no key encoding. It answers "what is this" and stops. The ASCII overview goes away
   with it — the simple mermaid does the same job and GitHub renders it natively.

4. **The detailed diagram moves to `architecture.md` and is trimmed there too.** The
   key-encoding block comes out of the Kafka node and becomes prose beneath the
   diagram. Nodes keep their names — both state stores, `DedupProcessor`,
   `AggregationTopology` — because that doc's reader wants exactly that.

5. **Assumptions stay in the README as a compact `Assumption | Basis` table.** An
   assumption is a statement of what the deliverable rests on, not rationale, and a
   reviewer looks for it directly. One line each. `design-notes.md` expands the
   load-bearing ones — in particular that `D` = debit is *consistent* across all 717
   sample records but not *verified*, since no `C` record exists anywhere in the
   sample, and that this became load-bearing only once fees were surfaced in the UI.

6. **Requirements traceability stays in the README.** It is the highest-value table
   for a reviewer holding the brief, it is a mapping rather than an argument, and it
   is already terse.

7. **The `wait-for-topic` operational note stays; its rationale moves.** Seeing
   `pfm-wait-for-topic` as `Exited (0)` looks like a failure and is not — a reader
   running the stack needs that on the page. Why the gate exists at all
   (`MissingSourceTopicException` kills the `StreamThread`, which does not recover)
   is rationale and moves to `design-notes.md`.

8. **The running-aggregate warning stays in the README.** It is a foot-gun that bites
   during operation, not a design argument. Trimmed to the `IMPORTANT` callout and the
   `docker compose down -v` remedy; the "this is the same property that makes
   re-ingesting the same file a no-op" explanation moves to `design-notes.md`.

## Constraints

- **No new prose.** Every line in `architecture.md` and `design-notes.md` is text moved
  from the current README, lightly re-headed for its new context. No claim gets
  invented, and no claim needs re-verification, because none is new. The factual
  verification table in
  [the README overhaul spec](2026-08-13-readme-overhaul-design.md) still stands.
- **No orphaned links.** The current README has internal anchors (`#using-your-own-file`,
  `#scalability`) and outbound links to all 9 specs, the per-module READMEs, source
  files and `docs/file-spec.md`. Every one is repointed to its new home or removed
  with its section. None is left dangling.
- **Documentation only.** No application code, no `docker-compose.yml`, no k8s
  manifests, no tests.

## Verification

Extract every markdown link target from `README.md`, `docs/architecture.md` and
`docs/design-notes.md`, and confirm each resolves — file paths against the working
tree, in-document anchors against the headings actually present. Confirm the README's
final line count is in the target range and that no removed section's content is
absent from both new docs.

## Out of scope

- The per-module READMEs (`common/`, `ingestion-service/`, `processing-service/`,
  `frontend/`, `k8s/`). They are accurate and scoped correctly; the root README keeps
  linking to them.
- The 9 per-slice specs. They are the archive and are not edited, summarised into, or
  superseded by the new docs.
- `docs/file-spec.md`.
