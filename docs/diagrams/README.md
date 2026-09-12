# Architecture diagrams

Two diagrams are generated from `generate_diagrams.py`:

| Diagram | Audience | Scope |
|---|---|---|
| `01-high-level-architecture` | mixed | Systems, data flow, quality, orchestration, and current consumers |
| `02-warehouse-dimensional-model` | technical | dbt layers, model grains, keys, and physical layout |

Each is checked in as `.drawio` and `.excalidraw`. Edit the Python source,
then regenerate both formats so they stay aligned:

```bash
uv run python docs/diagrams/generate_diagrams.py
```

Open `.drawio` files in [diagrams.net](https://app.diagrams.net/) or
`.excalidraw` files in [Excalidraw](https://excalidraw.com/).

For slides, export PNG at 2× scale with a transparent background. Do not edit
generated files directly; regeneration will overwrite those changes.
