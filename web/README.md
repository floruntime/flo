# Flo Console

The Flo dashboard web UI — a full rebuild implementing the **Console v2 ("calm")** design:
warm-dark, near-monochrome theme with a single sage accent, Inter / JetBrains Mono /
Collier-serif type, and flat borderless surfaces.

> **Status: visual-first.** Screens run on in-repo **mock data** (`src/lib/mock/`). The live
> Flo dashboard API (react-query / SSE) is **not** wired yet — that's a follow-up pass. The dev
> proxy to `localhost:9002` is left configured in `vite.config.ts` for that work.

```bash
npm install
npm run dev       # http://localhost:5173
npm run build     # tsc -b && vite build → dist/
```

## Layout

Everything is plain React + TypeScript + Vite. **No Tailwind** — the design's hand-tuned CSS is
ported as CSS variables + co-located `.css` files so values are easy to tweak.

```
src/
  styles/
    tokens.css          design tokens (:root vars), @font-face, base reset
    index.css           aggregator — @imports every co-located .css in cascade order
  lib/
    format.ts           crng / cfmt / tfmt / jhl helpers
    icons.tsx           <Icon paths="…"/> + the nav/section glyph maps + small icons
    cx.ts               tiny className joiner
    namespace.tsx       active-namespace context
    mock/               per-domain mock data (streams, kv, timeseries, compute, processing, workflows)
  components/           the reusable design-system primitives, one folder each:
    buttons/  inputs/  feedback/  layout/  overlay/  data/
    index.ts            barrel — import { Button, Field, Pill, Card, Modal, … } from '@/components'
  layout/
    Shell.tsx           header (brand + namespace switcher) + sidebar
  screens/              one folder per console screen (Overview, streams, kv, timeseries,
                        compute, processing, workflows)
  playground/
    Playground.tsx      live component gallery at /playground
  App.tsx               router — console routes + /playground
```

### Styling model

Each component/screen keeps its CSS **co-located** in its own folder. `src/styles/index.css`
`@import`s them all once (tokens → atoms → layout → screens → playground), so every class is
available globally while staying easy to find and edit. To tweak a component's look, edit the
`.css` next to it.

### Component playground

`/playground` is an in-app gallery of every primitive (tokens, buttons, inputs, feedback,
layout, overlay) with live, interactive variants — the place to design/tweak components in
isolation.

## Embedding into the `flo` binary

The binary embeds `web/dist/` via `src/node/dashboard/assets.zig`, which lists **content-hashed**
asset filenames. After a production build the hashes change, so that generated file must be
regenerated for the embedded dashboard to pick up this UI. That embed/deploy step is separate
from local `npm run dev` and is out of scope for the visual-first rebuild.
