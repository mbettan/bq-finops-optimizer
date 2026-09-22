# Engineering Specs

This directory holds **implementation-ready engineering specs**. Each spec is a self-contained unit of
work that can be opened as a GitHub Feature Request, assigned, and implemented without further design.

> [!NOTE]
> Specs live here rather than in `docs/` because `docs/` is the published GitHub Pages site.

## Layout

```
specs/
  README.md                 ← this file
  gap-28/                   ← one directory per epic
    README.md               ← epic index: sequencing, dependency graph, shared decisions
    GAP-28.0-….md           ← one file per independently shippable feature
```

## Lifecycle

| Status | Meaning |
| :--- | :--- |
| `DRAFT` | Being written. Not reviewable. |
| `IN REVIEW` | Under engineering review; open questions unresolved. |
| **`READY FOR IMPLEMENTATION`** | Reviewed, decisions locked, acceptance criteria testable. Safe to assign. |
| `IN PROGRESS` | Implementation underway; link the PR in the header. |
| `SHIPPED` | Merged and released; header records the release version. |
| `SUPERSEDED` | Replaced; header links the successor. |

## Required header fields

```
**Spec ID** · **Epic** · **Status** · **Module** · **Target Release**
**Depends On** · **Blocks** · **Estimated Effort** · **Owner** · **Last Updated**
```

`Module` must match one of the options in
[`.github/ISSUE_TEMPLATE/feature_request.yml`](../.github/ISSUE_TEMPLATE/feature_request.yml) so a spec
maps cleanly onto a filed feature request.

## Required sections

1. **Problem** — the user-visible symptom, not the technical task.
2. **Evidence** — every claim line-referenced to real code (`src/main.py:555`). No assumptions.
3. **Goals / Non-Goals** — explicit scope boundary.
4. **Design** — the decided approach, including options rejected and why.
5. **Implementation Tasks** — an ordered checklist.
6. **Acceptance Criteria** — numbered and *testable*; each maps to a test or a manual check.
7. **Risks & Mitigations**.
8. **Rollback**.
9. **Open Questions** — with a stated default if unanswered.

## House rules

* **Evidence over assertion.** If a spec describes code, it cites the file and line.
* **Customer-visible behavior changes** are called out in a `> [!WARNING]` block and require a release note.
* **New dependencies** require a recorded dependency-scan result *before* they are added.
* **Frontend changes land twice** — `static/` and `docs/static/` are byte-identical bundles.
* Specs are **anonymized**: no customer, partner or individual names.
