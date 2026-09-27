"""Convert the report's SVG figures to PDF for inclusion by pdfLaTeX.

pdfLaTeX cannot read SVG, and neither Inkscape nor rsvg-convert is guaranteed on a
marker's machine. Headless Chrome is, because the project already requires Docker
Desktop, and Chrome preserves the SVG as vector rather than rasterising it.

The trick is the @page size: it is set to the SVG's own viewBox in inches (CSS px
are 1/96 in), so the printed page is exactly the figure and no cropping step --
which would drag in Ghostscript -- is needed.

    python scripts/build_report_figures.py

Reads:  docs/report-assets/*.svg
Writes: docs/report/figures/*.pdf
"""
from __future__ import annotations

import pathlib
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent.parent
SVG_DIR = ROOT / "docs" / "report-assets"
OUT_DIR = ROOT / "docs" / "report" / "figures"

# Same list the LaTeX \includegraphics calls use. Keep in step with chapters/.
FIGURES = ["fig1-architecture", "fig2-consistency"]

CHROME_CANDIDATES = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "google-chrome",
    "chromium",
]


def find_chrome() -> str:
    for candidate in CHROME_CANDIDATES:
        if pathlib.Path(candidate).exists():
            return candidate
        found = shutil.which(candidate)
        if found:
            return found
    sys.exit("no Chrome or Edge found; install one, or export the SVGs to PDF by hand")


def svg_size_px(svg: str) -> tuple[float, float]:
    """Width and height in CSS pixels, preferring the viBox over the attributes.

    The viewBox is the authoritative coordinate space; width/height may carry
    units or be absent entirely.
    """
    box = re.search(r'viewBox="([\d.\s-]+)"', svg)
    if box:
        parts = [float(n) for n in box.group(1).split()]
        if len(parts) == 4:
            return parts[2], parts[3]
    width = re.search(r'\bwidth="([\d.]+)', svg)
    height = re.search(r'\bheight="([\d.]+)', svg)
    if width and height:
        return float(width.group(1)), float(height.group(1))
    sys.exit("cannot determine SVG size; add a viewBox")


def convert(chrome: str, name: str) -> None:
    svg_path = SVG_DIR / f"{name}.svg"
    svg = svg_path.read_text(encoding="utf-8")
    width_px, height_px = svg_size_px(svg)

    # CSS px -> inches. Chrome's print box is specified in real units, so this is
    # what makes the output page exactly the figure's bounding box.
    width_in = width_px / 96.0
    height_in = height_px / 96.0

    html = f"""<!doctype html>
<html><head><meta charset="utf-8"><style>
  @page {{ size: {width_in:.4f}in {height_in:.4f}in; margin: 0; }}
  html, body {{ margin: 0; padding: 0; }}
  svg {{ display: block; width: {width_in:.4f}in; height: {height_in:.4f}in; }}
</style></head><body>
{svg}
</body></html>"""

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = pathlib.Path(tmp)
        html_path = tmp_dir / f"{name}.html"
        html_path.write_text(html, encoding="utf-8")
        out_path = OUT_DIR / f"{name}.pdf"

        subprocess.run(
            [
                chrome,
                "--headless",
                "--disable-gpu",
                "--no-sandbox",
                # Chrome adds a date/URL header band unless this is passed.
                "--no-pdf-header-footer",
                f"--print-to-pdf={out_path}",
                html_path.as_uri(),
            ],
            check=True,
            # Chrome is chatty on stderr even on success.
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            # A wedged headless Chrome would otherwise hang the build.
            timeout=120,
        )

        if not out_path.exists():
            sys.exit(f"Chrome did not write {out_path}")
        print(f"  {name}.pdf  ({width_px:.0f}x{height_px:.0f} px -> "
              f"{width_in:.2f}x{height_in:.2f} in, {out_path.stat().st_size // 1024} KB)")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    chrome = find_chrome()
    print(f"converting figures with {pathlib.Path(chrome).name}")
    for name in FIGURES:
        convert(chrome, name)
    print(f"wrote {len(FIGURES)} figures to {OUT_DIR}")


if __name__ == "__main__":
    main()
