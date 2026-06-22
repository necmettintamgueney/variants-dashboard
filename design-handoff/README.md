# Design Handoff — Variant Grouping Launch Report

**For:** Claude Design
**From:** Necmettin Tamgüney (Product, Q-Commerce Content)
**Date:** June 2026

---

## What this is

A single-page static dashboard that reports on the Variant Grouping auto-grouping model launch. It is shown to leadership and partner teams (engineering, design), often over screen-share. It is built as one self-contained HTML file with inline CSS and a tabbed layout.

**Live page (for reference):** https://necmettintamgueney.github.io/variants-dashboard/variant_grouping_model_analysis.html

This folder contains everything you need:

- `variant_grouping_model_analysis.html` — the page to review and edit. One self-contained file, inline CSS, no external assets. Open it in a browser to view all 10 tabs.
- `README.md` — this brief.

Edit the HTML file directly and return the updated file. The live link above is only for reference; you do not need access to it or to any repository.

---

## What we are asking for

A design and content audit across **all tabs**, focused on two outcomes:

1. **Better user experience.** Clear visual hierarchy, easy to follow on a shared screen, comfortable reading sizes, sensible element order, consistent spacing and components.
2. **More self-explanatory content.** Anyone with zero prior context should be able to open a tab and understand what they are looking at without a verbal walkthrough. Add short framing where a section assumes knowledge the reader does not have.

Audit every tab, propose changes, and apply them where they clearly improve clarity. Where a change is a judgement call, leave a short note rather than guessing.

---

## Hard constraints (do not break these)

- **Do not change any data.** All numbers, percentages, counts, group names, product names, category labels, and table rows must stay exactly as they are. This dashboard reports real launch results. Only text framing (labels, headings, captions, helper sentences) and visual design may change.
- **Keep all logos and icons.** The Delivery Hero logo (inline SVG in the header) and any other provided icons or brand marks must be preserved as-is, never replaced, recoloured, or removed.
- **No AI-generated filler.** Every sentence must earn its place. No vague marketing phrasing, no padding, no "in today's fast-paced world" style intros. Plain, precise, professional. Avoid em-dashes.
- **Keep percentages clearly labelled.** Where a number is a percentage, it must read as a percentage with a self-explanatory label, not a bare figure.
- **Preserve the LIVE vs OURS-NEW distinction** wherever products are tagged. That tagging is meaningful and must not be flattened.
- **Stay within the existing design system.** Use the CSS variables and tokens already defined in the file (colours, type scale, radius, shadow). Do not introduce a new font or a new colour palette. Extend, do not replace.

---

## The tabs (10 total)

| # | Tab | What it covers |
|---|---|---|
| 1 | Executive Summary | Top-line KPIs, platform breakdown, quality signal, immediate actions |
| 2 | Detail: Overview | Full scale and quality metrics |
| 3 | What went wrong | Root cause analysis for the 55 corrected groups |
| 4 | By category | Edit rate heatmap by product category |
| 5 | All 55 edited groups | Filterable table of every corrected group |
| 6 | Products moved | Product-level correction log |
| 7 | Review coverage | Editor coverage by platform |
| 8 | Group Distribution | Group size and shape distribution, live data |
| 9 | Held-back groups | The groups blocked by the merge layer, with real examples and the decision mechanism |
| 10 | QS OS Learn | Learn template: hypothesis, metrics, qualitative feedback, learnings, decisions |

---

## Context the reader needs (so content can be made self-explanatory)

These are the concepts the dashboard assumes. Use them to add framing where a tab drops the reader in without setup.

- **The model.** An auto-grouping model clusters products into consumer-facing variant groups (for example, the same yoghurt in different sizes and flavours grouped under one product card).
- **Variant dimensions.** Groups split along axes: size only, flavour only, or size + flavour.
- **The PIM rule.** A group is only valid if every member shares the same product type, master category (L3), and brand.
- **Held-back groups.** Where one of our model's products already belonged to a live production group, we parked our version instead of editing live data. The "Held-back groups" tab explains this pile.
- **Missing-dimension / enrichment case.** Two groups can only merge if they split on the same axes. When one side carries a value (for example a flavour) the other side never set, that value must be found and written onto the members missing it before the merge. Reconciling existing values that already disagree (synonyms) is out of scope.
- **Decision mechanism.** Readable value: enrich. Ambiguous value: send to human review. Does not fit: split.

---

## Tab-by-tab focus areas

Treat these as starting prompts, not a fixed checklist. Apply judgement.

- **Executive Summary** — This is the first thing leadership sees. Make the headline outcome and the immediate actions impossible to miss. Check the KPI sizing and that each KPI says what it measures.
- **Detail: Overview** — Dense metrics. Make sure each metric has a plain label and a unit. Group related metrics visually.
- **What went wrong** — Root cause content. Make sure the reader understands these are the corrected minority, not the whole launch, before they read the failures.
- **By category** — Heatmap. Confirm the colour scale has a legend and that a reader knows whether high is good or bad.
- **All 55 edited groups / Products moved** — Long tables. Headers must be legible and the table must be scannable on a shared screen. Confirm the filter is discoverable.
- **Review coverage** — Confirm the reader understands what coverage means and why it matters.
- **Group Distribution** — Charts from live data. Confirm axes and units are labelled and the takeaway is stated, not just shown.
- **Held-back groups** — Recently reworked. Has numbered sections, a 3-step intro flow, real examples (including a partial-gap enrichment case), and a decision mechanism. Check it reads cleanly start to finish for someone new.
- **QS OS Learn** — Structured template. Keep the template structure intact; improve readability within it.

---

## Working notes for the editor

- The page is one HTML file. Tabs are shown and hidden with a `showTab(id, el)` function; nav links carry the tab ids (`exec`, `overview`, `issues`, `heatmap`, `allgroups`, `moves`, `review`, `dist`, `changes`, `learn`).
- The design tokens live in the `:root` block at the top: brand red `--dh-red` (#D61F26), green / blue / purple / amber accents with tint variants, surface and border tokens, ink text tokens, radius and shadow tokens, and a responsive type scale built with `clamp()`.
- Font is Outfit throughout. Keep it.
- Test on a typical laptop screen width and a screen-share zoom level. The page should not require zooming to read.
- Edit `variant_grouping_model_analysis.html` directly. It is fully self-contained, so all edits live in that one file.

---

## Definition of done

- Every tab reviewed; clear improvements applied; judgement calls flagged.
- A reader with no context can understand each tab on their own.
- All data, logos, and icons unchanged.
- No filler text. Clean, plain, professional throughout.
- Updated `variant_grouping_model_analysis.html` returned, opening cleanly in a browser.
