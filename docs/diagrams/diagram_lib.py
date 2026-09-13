"""Shared Excalidraw emitter for the architecture diagram."""

import json
from dataclasses import dataclass, field
from pathlib import Path

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
    "note": ("#f8f9fa", "#adb5bd"),
    "zone": ("#ffffff", "#adb5bd"),
}

# Zone titles must read clearly; the dashed border stays light so it recedes.
ZONE_LABEL = "#343a40"

# Component names take their zone's hue so the eye can group them, darkened until
# each clears 4.5:1 on its own fill. The border colours themselves are too light
# for text at this size (orange 2.3:1, teal 2.9:1).
HEAD_COLOR = {
    "source": "#343a40",   # 10.9:1
    "storage": "#a5390a",  #  6.2:1
    "move": "#5f3dc4",     #  6.3:1
    "wh": "#1864ab",       #  5.5:1
    "mart": "#237032",     #  5.7:1
    "consume": "#087f5b",  #  4.7:1
    "quality": "#c92a2a",  #  4.5:1
    "orch": "#a5390a",     #  6.1:1
}


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
    head: bool = False  # render the first label line as a larger component name


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

# Excalidraw text elements carry no font weight and only one size each, so a
# component name is emitted as its own, larger element rather than as bold.
_HEAD_BUMP = 3
_HEAD_CHAR_W = 0.55  # only used to keep an enlarged name on a single line


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

        if b.head and not b.zone and len(lines) > 1:
            head_line, body_lines = lines[0], lines[1:]
            head_fs = fs + _HEAD_BUMP
            # shrink back rather than let a longer name wrap and break the layout
            while head_fs > fs and len(head_line) * head_fs * _HEAD_CHAR_W > b.w - 16:
                head_fs -= 1
            head_h = round(head_fs * _LINE_H, 1)
            body_h = round(len(body_lines) * fs * _LINE_H, 1)
            gap = 4
            top = b.y + (b.h - (head_h + gap + body_h)) / 2

            rect["boundElements"] = bound.get(b.id, [])
            els.append(rect)

            head_color = HEAD_COLOR.get(b.kind, "#1e1e1e")
            for eid, y, h, size, txt, colour in (
                (tid, top, head_h, head_fs, head_line, head_color),
                (b.id + "_b", top + head_h + gap, body_h, fs, "\n".join(body_lines), "#1e1e1e"),
            ):
                t = _common(eid, b.x, y, b.w, h, colour, "transparent")
                t.update(
                    {
                        "type": "text",
                        "text": txt,
                        "originalText": txt,
                        "fontSize": size,
                        "fontFamily": 2,
                        "textAlign": b.align,
                        "verticalAlign": "top",
                        "containerId": None,
                        "lineHeight": _LINE_H,
                        "autoResize": False,
                        "roundness": None,
                    }
                )
                els.append(t)
            continue

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
    (outdir / f"{stem}.excalidraw").write_text(to_excalidraw(d))
    print(f"  {stem}.excalidraw   ({len(d.boxes)} boxes, {len(d.edges)} edges)")
