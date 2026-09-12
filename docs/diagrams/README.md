# Architecture diagram

The checked-in architecture diagram is
`01-high-level-architecture.excalidraw`. It covers systems, data flow,
quality, orchestration, and current consumers. The dimensional model is
documented in [Star schema](../star_schema.md), including its Mermaid diagram.

Edit the Python source, then regenerate the Excalidraw file:

```bash
uv run python docs/diagrams/generate_diagrams.py
```

Open the file in [Excalidraw](https://excalidraw.com/).

For slides, export PNG at 2× scale with a transparent background. Do not edit
generated files directly; regeneration will overwrite those changes.
