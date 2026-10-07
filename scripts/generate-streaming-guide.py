#!/usr/bin/env python3
"""Gera o PDF do roteiro de Streaming a partir da fonte Markdown editável."""

from __future__ import annotations

import html
import re
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    ListFlowable,
    ListItem,
    PageBreak,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "docs" / "roteiro-apresentacao-streaming.md"
OUTPUT = ROOT / "docs" / "roteiro-apresentacao-streaming.pdf"

INK = colors.HexColor("#002B3A")
GREEN = colors.HexColor("#00684A")
MINT = colors.HexColor("#DFF5EB")
GRID = colors.HexColor("#B7C9D0")
LIGHT = colors.HexColor("#F2F6F7")
MUTED = colors.HexColor("#58717B")


def inline(text: str) -> str:
    value = html.escape(text, quote=False)
    value = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", value)
    value = re.sub(r"`(.+?)`", r'<font name="Courier">\1</font>', value)
    return value


class GuideDoc(BaseDocTemplate):
    def __init__(self):
        super().__init__(
            str(OUTPUT), pagesize=A4,
            leftMargin=18 * mm, rightMargin=18 * mm,
            topMargin=16 * mm, bottomMargin=16 * mm,
            title="Roteiro - módulo Streaming", author="Atlas Feature Showcase",
        )
        frame = Frame(self.leftMargin, self.bottomMargin, self.width, self.height, id="body")
        self.addPageTemplates(PageTemplate(id="guide", frames=[frame], onPage=self.footer))

    @staticmethod
    def footer(canvas, doc):
        canvas.saveState()
        canvas.setStrokeColor(colors.HexColor("#00A35C"))
        canvas.setLineWidth(0.5)
        canvas.line(doc.leftMargin, 12 * mm, A4[0] - doc.rightMargin, 12 * mm)
        canvas.setFont("Helvetica", 7)
        canvas.setFillColor(MUTED)
        canvas.drawString(doc.leftMargin, 8 * mm, "Atlas Feature Showcase - módulo Streaming")
        canvas.drawRightString(A4[0] - doc.rightMargin, 8 * mm, str(doc.page))
        canvas.restoreState()


def styles():
    base = getSampleStyleSheet()
    return {
        "h1": ParagraphStyle("h1", parent=base["Title"], fontName="Helvetica-Bold", fontSize=20,
                             leading=23, textColor=INK, alignment=TA_LEFT, spaceAfter=5),
        "h2": ParagraphStyle("h2", parent=base["BodyText"], fontName="Helvetica-Bold", fontSize=12.5,
                             leading=15, textColor=GREEN, spaceBefore=6, spaceAfter=5, keepWithNext=0),
        "body": ParagraphStyle("body", parent=base["BodyText"], fontName="Helvetica", fontSize=8.4,
                               leading=11.2, textColor=INK, spaceAfter=4),
        "bullet": ParagraphStyle("bullet", parent=base["BodyText"], fontName="Helvetica", fontSize=8.2,
                                 leading=10.8, textColor=INK),
        "cell": ParagraphStyle("cell", parent=base["BodyText"], fontName="Helvetica", fontSize=7.3,
                               leading=9.1, textColor=INK),
        "head": ParagraphStyle("head", parent=base["BodyText"], fontName="Helvetica-Bold", fontSize=7.4,
                               leading=9.2, textColor=colors.white),
    }


def make_table(rows: list[list[str]], style_map) -> Table:
    count = len(rows[0])
    weights = [max(7, min(34, max(len(row[i]) for row in rows))) for i in range(count)]
    total = sum(weights)
    widths = [174 * mm * weight / total for weight in weights]
    data = []
    for row_index, row in enumerate(rows):
        paragraph_style = style_map["head"] if row_index == 0 else style_map["cell"]
        data.append([Paragraph(inline(cell), paragraph_style) for cell in row])
    table = Table(data, colWidths=widths, repeatRows=1, hAlign="LEFT")
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), INK),
        ("GRID", (0, 0), (-1, -1), 0.35, GRID),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT]),
    ]))
    return table


def parse_markdown(text: str):
    style_map = styles()
    lines = text.splitlines()
    story = []
    index = 0
    while index < len(lines):
        line = lines[index].strip()
        if not line:
            index += 1
            continue
        if line == "<!-- pagebreak -->":
            story.append(PageBreak())
            index += 1
            continue
        if line.startswith("# "):
            story.append(Paragraph(inline(line[2:]), style_map["h1"]))
            index += 1
            continue
        if line.startswith("## "):
            story.append(Paragraph(inline(line[3:]), style_map["h2"]))
            story.append(Spacer(1, 1.5))
            index += 1
            continue
        if line.startswith("| "):
            raw_rows = []
            while index < len(lines) and lines[index].strip().startswith("|"):
                cells = [cell.strip() for cell in lines[index].strip().strip("|").split("|")]
                if not all(re.fullmatch(r"[-: ]+", cell) for cell in cells):
                    raw_rows.append(cells)
                index += 1
            story.extend([make_table(raw_rows, style_map), Spacer(1, 4)])
            continue
        if line.startswith("- "):
            items = []
            while index < len(lines) and lines[index].strip().startswith("- "):
                items.append(ListItem(Paragraph(inline(lines[index].strip()[2:]), style_map["bullet"]),
                                      leftIndent=9, bulletColor=GREEN))
                index += 1
            story.append(ListFlowable(items, bulletType="bullet", leftIndent=12, bulletFontSize=5, spaceAfter=4))
            continue
        paragraph = [line]
        index += 1
        while index < len(lines):
            candidate = lines[index].strip()
            if not candidate or candidate.startswith(("#", "|", "- ", "<!-- pagebreak")):
                break
            paragraph.append(candidate)
            index += 1
        story.append(Paragraph(inline(" ".join(paragraph)), style_map["body"]))
    return story


def main() -> None:
    GuideDoc().build(parse_markdown(SOURCE.read_text(encoding="utf-8")))
    print(OUTPUT)


if __name__ == "__main__":
    main()
