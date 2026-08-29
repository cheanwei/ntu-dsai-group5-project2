# Architecture diagrams

Currently checked in: **`01-high-level-architecture.excalidraw`** only.

System topology — how data moves between Kaggle, GCS, dlt, BigQuery, and the
analytics layer, treating the warehouse as one box. Executive / mixed audience.

Open it at [excalidraw.com](https://excalidraw.com) (**Menu → Open**) or with the
*Excalidraw* VS Code extension.

## What is not checked in, and how to get it back

`generate_diagrams.py` still defines **both** diagrams and emits **both**
formats. Nothing was lost — the other three outputs are simply not committed
yet:

| Output | Status |
|---|---|
| `01-high-level-architecture.excalidraw` | checked in |
| `01-high-level-architecture.drawio` | regenerate on demand |
| `02-warehouse-dimensional-model.excalidraw` | regenerate on demand |
| `02-warehouse-dimensional-model.drawio` | regenerate on demand |

Diagram 02 is the inside of the warehouse box: dbt layering and the fact
constellation, with grains, keys, and physical layout (§5). Diagram 01 is what
you put in front of the CEO; 02 is what you put in front of the CTO. Neither
should try to be the other — that is the whole reason they are separate files.

```bash
python3 docs/diagrams/generate_diagrams.py
```

That writes all four files. Commit only the ones you want; the rest can be
deleted again without losing anything, since the content lives in the script.

- `generate_diagrams.py` — the diagram content (boxes, edges, coordinates)
- `diagram_lib.py` — the two emitters and the shared colour palette

Both formats come from one source of truth, so they cannot drift apart.
Hand-editing an output file is possible but means the next regeneration
silently discards your changes.

## Exporting for the slide deck

From Excalidraw: **Menu → Export image**, scale 2×, untick *Background* for a
slide overlay.

From draw.io (after regenerating the `.drawio`): **File → Export as → PNG**,
zoom 200%, tick *Transparent background*.
