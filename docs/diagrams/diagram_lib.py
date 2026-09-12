"""Shared emitters: one diagram model -> draw.io (.drawio) and Excalidraw (.excalidraw)."""

import json
from dataclasses import dataclass, field
from pathlib import Path
from xml.sax.saxutils import escape

# ---------------------------------------------------------------- palette ---
# Excalidraw-native colours so the files look at home in either tool.
PALETTE = {
    "source": ("#f8f9fa", "#868e96"),
    "storage": ("#fff9db", "#f08c00"),
    "move": ("#f3f0ff", "#6741d9"),
    "wh": ("#e7f5ff", "#1971c2"),
    "mart": ("#ebfbee", "#2f9e44"),
    "consume": ("#e6fcf5", "#0ca678"),
    "quality": ("#ffe3e3", "#e03131"),
    "orch": ("#fff4e6", "#e8590c"),
    "fact": ("#e7f5ff", "#1971c2"),
    "dim": ("#fff9db", "#f08c00"),
    "note": ("#f8f9fa", "#adb5bd"),
    "zone": ("#ffffff", "#adb5bd"),
    "layer": ("#f3f0ff", "#6741d9"),
}

# Zone titles must read clearly; the dashed border stays light so it recedes.
ZONE_LABEL = "#343a40"


@dataclass
class Box:
    id: str
    x: int
    y: int
    w: int
    h: int
    label: str
    kind: str = "wh"
    zone: bool = False  # dashed container, rendered behind everything
    font: int = 12
    align: str = "center"


@dataclass
class Edge:
    id: str
    src: str
    dst: str
    pts: list  # explicit polyline for Excalidraw
    label: str = ""
    dashed: bool = False
    kind: str = "solid"


@dataclass
class Diagram:
    name: str
    width: int
    height: int
    boxes: list = field(default_factory=list)
    edges: list = field(default_factory=list)


# ---------------------------------------------------------------- draw.io ---
def _dio_label(text: str) -> str:
    """Build the HTML draw.io renders, then escape it whole for the XML attribute."""
    lines = text.split("\n")
    html = "<b>" + lines[0] + "</b>"
    if len(lines) > 1:
        html += "<br>" + "<br>".join(lines[1:])
    return escape(html, {'"': "&quot;"})


def to_drawio(d: Diagram) -> str:
    cells = []
    for b in sorted(d.boxes, key=lambda b: not b.zone):  # zones first = behind
        fill, stroke = PALETTE[b.kind]
        if b.zone:
            style = (
                f"rounded=1;arcSize=6;whiteSpace=wrap;html=1;dashed=1;dashPattern=8 6;"
                f"fillColor=none;strokeColor={stroke};strokeWidth=2;"
                f"verticalAlign=top;align=left;spacingLeft=12;spacingTop=6;"
                f"fontSize={b.font};fontStyle=1;fontColor={ZONE_LABEL};"
            )
        else:
            style = (
                f"rounded=1;arcSize=8;whiteSpace=wrap;html=1;"
                f"fillColor={fill};strokeColor={stroke};strokeWidth=2;"
                f"align={b.align};verticalAlign=middle;spacingLeft=8;spacingRight=8;"
                f"fontSize={b.font};fontColor=#212529;"
            )
        cells.append(
            f'        <mxCell id="{b.id}" value="{_dio_label(b.label)}" style="{style}" '
            f'vertex="1" parent="1">\n'
            f'          <mxGeometry x="{b.x}" y="{b.y}" width="{b.w}" '
            f'height="{b.h}" as="geometry"/>\n'
            f"        </mxCell>"
        )
    for e in d.edges:
        dash = "dashed=1;dashPattern=6 6;" if e.dashed else ""
        style = (
            f"edgeStyle=orthogonalEdgeStyle;rounded=1;html=1;jettySize=auto;"
            f"strokeColor=#495057;strokeWidth=2;endArrow=blockThin;endFill=1;{dash}"
            f"fontSize=10;fontColor=#495057;labelBackgroundColor=#ffffff;"
        )
        cells.append(
            f'        <mxCell id="{e.id}" value="{escape(e.label)}" style="{style}" '
            f'edge="1" parent="1" source="{e.src}" target="{e.dst}">\n'
            f'          <mxGeometry relative="1" as="geometry"/>\n'
            f"        </mxCell>"
        )
    body = "\n".join(cells)
    return (
        '<mxfile host="app.diagrams.net" type="device">\n'
        f'  <diagram name="{escape(d.name)}" id="{d.name.replace(" ", "-").lower()}">\n'
        f'    <mxGraphModel dx="1422" dy="800" grid="1" gridSize="10" guides="1" tooltips="1" '
        f'connect="1" arrows="1" fold="1" page="1" pageScale="1" pageWidth="{d.width}" '
        f'pageHeight="{d.height}" math="0" shadow="0">\n'
        "      <root>\n"
        '        <mxCell id="0"/>\n'
        '        <mxCell id="1" parent="0"/>\n'
        f"{body}\n"
        "      </root>\n"
        "    </mxGraphModel>\n"
        "  </diagram>\n"
        "</mxfile>\n"
    )


# ------------------------------------------------------------- excalidraw ---
_SEED = [1000]


def _nonce():
    _SEED[0] += 7919
    return _SEED[0]


def _common(eid, x, y, w, h, stroke, fill, dashed=False):
    return {
        "id": eid,
        "x": x,
        "y": y,
        "width": w,
        "height": h,
        "angle": 0,
        "strokeColor": stroke,
        "backgroundColor": fill,
        "fillStyle": "solid",
        "strokeWidth": 2,
        "strokeStyle": "dashed" if dashed else "solid",
        "roughness": 1,
        "opacity": 100,
        "groupIds": [],
        "frameId": None,
        "roundness": {"type": 3},
        "seed": _nonce(),
        "version": 1,
        "versionNonce": _nonce(),
        "isDeleted": False,
        "boundElements": [],
        "updated": 1,
        "link": None,
        "locked": False,
    }


_CHAR_W = 0.58  # Helvetica average advance / font size
_LINE_H = 1.25


def to_excalidraw(d: Diagram) -> str:
    els = []
    bound = {}  # box id -> list of boundElements entries

    for e in d.edges:
        bound.setdefault(e.src, []).append({"id": e.id, "type": "arrow"})
        bound.setdefault(e.dst, []).append({"id": e.id, "type": "arrow"})

    for b in sorted(d.boxes, key=lambda b: not b.zone):
        fill, stroke = PALETTE[b.kind]
        rect = _common(
            b.id, b.x, b.y, b.w, b.h, stroke, "transparent" if b.zone else fill, dashed=b.zone
        )
        rect["type"] = "rectangle"
        tid = b.id + "_t"
        rect["boundElements"] = [{"id": tid, "type": "text"}] + bound.get(b.id, [])

        lines = b.label.split("\n")
        fs = b.font + 2
        th = round(len(lines) * fs * _LINE_H, 1)
        tw = round(max(len(line) for line in lines) * fs * _CHAR_W, 1)
        valign = "top" if b.zone else "middle"
        text = _common(
            tid,
            b.x + 12,
            b.y + 8 if b.zone else b.y + (b.h - th) / 2,
            min(tw, b.w - 24),
            th,
            ZONE_LABEL if b.zone else "#1e1e1e",
            "transparent",
        )
        text.update(
            {
                "type": "text",
                "text": b.label,
                "originalText": b.label,
                "fontSize": fs,
                "fontFamily": 2,
                "textAlign": "left" if b.zone else b.align,
                "verticalAlign": valign,
                "containerId": b.id,
                "lineHeight": _LINE_H,
                "autoResize": False,
                "roundness": None,
            }
        )
        els.append(rect)
        els.append(text)

    for e in d.edges:
        xs = [p[0] for p in e.pts]
        ys = [p[1] for p in e.pts]
        ox, oy = e.pts[0]
        arrow = _common(
            e.id, ox, oy, max(xs) - min(xs), max(ys) - min(ys), "#495057", "transparent"
        )
        arrow.update(
            {
                "type": "arrow",
                "strokeStyle": "dashed" if e.dashed else "solid",
                "roundness": {"type": 2},
                "points": [[p[0] - ox, p[1] - oy] for p in e.pts],
                "lastCommittedPoint": None,
                "startBinding": {"elementId": e.src, "focus": 0, "gap": 4},
                "endBinding": {"elementId": e.dst, "focus": 0, "gap": 4},
                "startArrowhead": None,
                "endArrowhead": "arrow",
                "elbowed": False,
            }
        )
        els.append(arrow)

        if e.label:
            fs = 11
            tw = round(len(e.label) * fs * _CHAR_W, 1)
            n = len(e.pts)
            if n % 2:
                mid = e.pts[n // 2]
            else:  # true midpoint, not the arrowhead
                a, c = e.pts[n // 2 - 1], e.pts[n // 2]
                mid = ((a[0] + c[0]) / 2, (a[1] + c[1]) / 2)
            lt = _common(
                e.id + "_l",
                mid[0] - tw / 2,
                mid[1] - 24,
                tw,
                fs * _LINE_H,
                "#495057",
                "transparent",
            )
            lt.update(
                {
                    "type": "text",
                    "text": e.label,
                    "originalText": e.label,
                    "fontSize": fs,
                    "fontFamily": 2,
                    "textAlign": "center",
                    "verticalAlign": "middle",
                    "containerId": None,
                    "lineHeight": _LINE_H,
                    "autoResize": True,
                    "roundness": None,
                }
            )
            els.append(lt)

    return json.dumps(
        {
            "type": "excalidraw",
            "version": 2,
            "source": "https://excalidraw.com",
            "elements": els,
            "appState": {"gridSize": None, "viewBackgroundColor": "#ffffff"},
            "files": {},
        },
        indent=2,
    )


def emit(d: Diagram, stem: str, outdir: Path):
    (outdir / f"{stem}.drawio").write_text(to_drawio(d))
    (outdir / f"{stem}.excalidraw").write_text(to_excalidraw(d))
    print(f"  {stem}.drawio  +  {stem}.excalidraw   ({len(d.boxes)} boxes, {len(d.edges)} edges)")
