"""Build docs/report.pdf from the LaTeX source in docs/report/.

The report follows the Department of Electrical and Information Engineering
report template, which needs things HTML cannot do: roman-numbered front
matter, a page-accurate table of contents, running chapter headers and a
"Page N of M" footer. So the source of truth is LaTeX, not Markdown.

    python scripts/build_report_pdf.py          # figures + full LaTeX cycle
    python scripts/build_report_pdf.py --quick  # skip bibtex (no new citations)

Four passes are needed, and each one is needed for a reason:
  1. pdflatex  -- writes .aux with the citation keys and label page numbers
  2. bibtex    -- resolves those keys against bibliography.bib
  3. pdflatex  -- pulls the bibliography in, which moves every page after it
  4. pdflatex  -- fixes the contents list and \\pageref{LastPage} after that shift

Output: docs/report.pdf (copied out of the build directory, so the committed
artefact sits next to the other docs rather than among LaTeX scratch files).
"""
from __future__ import annotations

import pathlib
import shutil
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
SRC_DIR = ROOT / "docs" / "report"
TEX = "report.tex"
BUILT_PDF = SRC_DIR / "report.pdf"
OUT_PDF = ROOT / "docs" / "report.pdf"


def require_pdflatex() -> None:
    if shutil.which("pdflatex"):
        return
    sys.exit(
        "pdflatex not found. Install TeX Live (or MiKTeX) and re-run, or build\n"
        "the report by uploading docs/report/ to Overleaf and compiling with pdfLaTeX."
    )


def run(cmd: list[str], label: str) -> subprocess.CompletedProcess[str]:
    print(f"  {label}")
    return subprocess.run(
        cmd,
        cwd=SRC_DIR,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        # A LaTeX error under -interaction=nonstopmode still terminates; this
        # only guards against a genuinely wedged process.
        timeout=600,
    )


def latex_pass(label: str) -> subprocess.CompletedProcess[str]:
    return run(["pdflatex", "-interaction=nonstopmode", TEX], label)


def report_problems() -> None:
    """Surface the two failure modes that are invisible in a successful build.

    An undefined reference renders as '??' and an overfull hbox runs text past
    the margin; pdflatex exits 0 in both cases, so the build has to look.
    """
    log = (SRC_DIR / "report.log").read_text(encoding="utf-8", errors="replace")

    undefined = [
        line for line in log.splitlines()
        if "Warning" in line and ("undefined" in line or "Citation" in line)
    ]
    overfull = [
        line for line in log.splitlines()
        if line.startswith("Overfull \\hbox")
    ]

    if undefined:
        print(f"\n  {len(undefined)} unresolved reference(s):")
        for line in undefined[:10]:
            print(f"    {line.strip()}")
    if overfull:
        print(f"\n  {len(overfull)} overfull hbox(es) -- text may run past the margin:")
        for line in overfull[:10]:
            print(f"    {line.strip()}")
    if not undefined and not overfull:
        print("  no unresolved references, no overfull boxes")


def main() -> None:
    quick = "--quick" in sys.argv
    require_pdflatex()

    print("building report figures")
    subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "build_report_figures.py")],
        check=True,
    )

    print(f"building {OUT_PDF.relative_to(ROOT)} from LaTeX")
    result = latex_pass("pdflatex (pass 1/4)")
    if not (SRC_DIR / "report.aux").exists():
        print(result.stdout[-4000:])
        sys.exit("pdflatex produced no .aux; see output above")

    if quick:
        print("  skipping bibtex (--quick)")
    else:
        bib = run(["bibtex", "report"], "bibtex")
        for line in bib.stdout.splitlines():
            if "Warning" in line or "Error" in line:
                print(f"    {line.strip()}")

    latex_pass("pdflatex (pass 3/4)")
    final = latex_pass("pdflatex (pass 4/4)")

    if not BUILT_PDF.exists():
        print(final.stdout[-4000:])
        sys.exit("no PDF produced; see the pdflatex output above")

    report_problems()

    shutil.copyfile(BUILT_PDF, OUT_PDF)
    pages = "?"
    for line in final.stdout.splitlines():
        if "Output written on" in line and "pages" in line:
            pages = line.split("(")[1].split(" pages")[0]
    print(f"\nwrote {OUT_PDF}  ({pages} pages, {OUT_PDF.stat().st_size // 1024} KB)")


if __name__ == "__main__":
    main()
