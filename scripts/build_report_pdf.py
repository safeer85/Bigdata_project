"""Render docs/report.md to a print-ready PDF.

The assignment requires the report as a PDF. Rather than depend on pandoc or a
LaTeX toolchain (neither is guaranteed on a marker's machine), this converts
Markdown to styled HTML and drives headless Chrome, which ships with Docker
Desktop's host anyway and is present on virtually every Windows/macOS machine.

    python scripts/build_report_pdf.py

Output: docs/report.pdf

SVG figures are INLINED into the HTML rather than referenced. Chrome's headless
PDF renderer will not load local file:// sub-resources reliably, so a referenced
SVG silently renders as a broken-image box -- which is exactly the kind of defect
you only notice after submitting.
"""
from __future__ import annotations

import base64
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent.parent
REPORT_MD = ROOT / "docs" / "report.md"
OUT_PDF = ROOT / "docs" / "report.pdf"

CHROME_CANDIDATES = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "google-chrome",
    "chromium",
]

CSS = """
@page { size: A4; margin: 18mm 16mm 20mm 16mm; }
* { box-sizing: border-box; }
body {
  font-family: "Segoe UI", -apple-system, Helvetica, Arial, sans-serif;
  font-size: 10.5pt; line-height: 1.55; color: #1a202c; margin: 0;
  -webkit-print-color-adjust: exact; print-color-adjust: exact;
}
h1 { font-size: 21pt; margin: 0 0 4pt; color: #0f172a; line-height: 1.2; }
h1 + p em, h1 + p strong { color: #475569; }
h2 {
  font-size: 14pt; margin: 20pt 0 8pt; padding-bottom: 4pt; color: #0f172a;
  border-bottom: 1.5pt solid #cbd5e1; page-break-after: avoid;
}
h3 { font-size: 11.5pt; margin: 14pt 0 5pt; color: #1e293b; page-break-after: avoid; }
p { margin: 0 0 8pt; orphans: 3; widows: 3; }
ul, ol { margin: 0 0 8pt; padding-left: 16pt; }
li { margin-bottom: 3pt; }
strong { color: #0f172a; }
code {
  font-family: Consolas, "Courier New", monospace; font-size: 9pt;
  background: #f1f5f9; padding: 1pt 3pt; border-radius: 2pt; color: #0f172a;
}
pre {
  background: #f8fafc; border: 0.7pt solid #cbd5e1; border-left: 2.5pt solid #64748b;
  padding: 7pt 9pt; border-radius: 3pt; overflow-x: auto;
  page-break-inside: avoid; margin: 0 0 9pt;
}
pre code { background: none; padding: 0; font-size: 8.6pt; line-height: 1.45; }
table {
  border-collapse: collapse; width: 100%; margin: 0 0 10pt;
  font-size: 9.2pt; page-break-inside: avoid;
}
th {
  background: #f1f5f9; text-align: left; padding: 5pt 6pt;
  border-bottom: 1.2pt solid #94a3b8; font-weight: 700; color: #0f172a;
}
td { padding: 4.5pt 6pt; border-bottom: 0.5pt solid #e2e8f0; vertical-align: top; }
tr:nth-child(even) td { background: #fafbfc; }
blockquote {
  margin: 0 0 9pt; padding: 6pt 10pt; background: #eff6ff;
  border-left: 2.5pt solid #2563eb; color: #1e3a8a;
}
blockquote p { margin: 0; }
hr { border: none; border-top: 0.7pt solid #e2e8f0; margin: 14pt 0; }
figure, .figure { margin: 10pt 0 12pt; page-break-inside: avoid; text-align: center; }
svg { max-width: 100%; height: auto; }
.figcaption { font-size: 8.8pt; color: #475569; margin-top: 4pt; text-align: left; }
h2 { page-break-before: auto; }
"""


def find_chrome() -> str:
    for candidate in CHROME_CANDIDATES:
        if os.path.sep in candidate or ":" in candidate:
            if os.path.exists(candidate):
                return candidate
        else:
            found = shutil.which(candidate)
            if found:
                return found
    raise SystemExit(
        "Could not find Chrome or Edge. Install one, or convert docs/report.md\n"
        "with pandoc:  pandoc docs/report.md -o docs/report.pdf"
    )


def inline_svgs(md: str) -> str:
    """Replace ![alt](path.svg) with the SVG markup itself.

    Inlined rather than linked because headless Chrome does not reliably fetch
    local file:// sub-resources when printing to PDF.
    """
    def repl(match: re.Match) -> str:
        rel = match.group(2)
        path = (REPORT_MD.parent / rel).resolve()
        if not path.exists():
            print(f"  WARNING: figure not found, leaving as-is: {rel}")
            return match.group(0)
        svg = path.read_text(encoding="utf-8")
        svg = re.sub(r"<\?xml[^>]*\?>", "", svg).strip()
        print(f"  inlined {path.name}")
        return f'<div class="figure">{svg}</div>'

    return re.sub(r"!\[([^\]]*)\]\(([^)]+\.svg)\)", repl, md)


def main() -> int:
    if not REPORT_MD.exists():
        raise SystemExit(f"{REPORT_MD} not found")

    try:
        import markdown
    except ImportError:
        raise SystemExit("pip install markdown")

    print("building docs/report.pdf")
    md_text = REPORT_MD.read_text(encoding="utf-8")
    md_text = inline_svgs(md_text)

    # Protect the inlined SVG blocks from the Markdown parser, which would
    # otherwise wrap stray lines in <p> and mangle the markup.
    blocks: list[str] = []

    def stash(match: re.Match) -> str:
        blocks.append(match.group(0))
        return f"\n\nSVGPLACEHOLDER{len(blocks) - 1}\n\n"

    md_text = re.sub(r'<div class="figure">.*?</div>', stash, md_text, flags=re.S)

    html_body = markdown.markdown(
        md_text,
        extensions=["tables", "fenced_code", "sane_lists", "attr_list"],
    )

    for index, block in enumerate(blocks):
        html_body = html_body.replace(
            f"<p>SVGPLACEHOLDER{index}</p>", block
        ).replace(f"SVGPLACEHOLDER{index}", block)

    # Italic figure captions immediately after a figure become .figcaption.
    html_body = re.sub(
        r"(</div>)\s*<p><em>(.*?)</em></p>",
        r'\1<div class="figcaption">\2</div>',
        html_body,
        flags=re.S,
    )

    html = (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<title>Fleet operations - a Lambda-architecture data pipeline</title>"
        f"<style>{CSS}</style></head><body>{html_body}</body></html>"
    )

    with tempfile.TemporaryDirectory() as tmp:
        html_path = pathlib.Path(tmp) / "report.html"
        html_path.write_text(html, encoding="utf-8")

        chrome = find_chrome()
        print(f"  rendering with {pathlib.Path(chrome).name}")

        if OUT_PDF.exists():
            OUT_PDF.unlink()

        result = subprocess.run(
            [
                chrome, "--headless", "--disable-gpu", "--no-sandbox",
                "--run-all-compositor-stages-before-draw",
                "--virtual-time-budget=10000",
                f"--print-to-pdf={OUT_PDF}",
                "--no-pdf-header-footer",
                html_path.as_uri(),
            ],
            capture_output=True, text=True, timeout=180,
        )

        if not OUT_PDF.exists():
            print(result.stdout[-2000:])
            print(result.stderr[-2000:])
            raise SystemExit("Chrome did not produce a PDF")

    size_kb = OUT_PDF.stat().st_size / 1024
    print(f"\nwrote {OUT_PDF}  ({size_kb:.0f} KB)")
    print("Open it and check the figures rendered before submitting.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
