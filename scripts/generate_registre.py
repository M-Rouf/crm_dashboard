import os
import re
from datetime import date, datetime
from html import escape
from typing import Iterable

import pdfkit


def _format_money(amount) -> str:
    return f"{float(amount):.2f}"


def _format_date(value) -> str:
    if isinstance(value, datetime):
        return value.strftime("%d/%m/%Y")
    if isinstance(value, date):
        return value.strftime("%d/%m/%Y")
    return str(value or "")


def _remove_marked_block(html_content: str, marker: str) -> str:
    pattern = rf"\s*<!-- {marker}-start -->.*?<!-- {marker}-end -->"
    return re.sub(pattern, "", html_content, count=1, flags=re.DOTALL)


def _build_item_rows(items: Iterable[dict]) -> str:
    rows = []
    for item in items:
        flux = (item.get("flux") or "").strip().lower()
        type_label = "Achat" if flux == "achat" else "Vente"
        tag_class = "tag-achat" if flux == "achat" else "tag-vente"
        rows.append(
            "          <tr>\n"
            f"            <td>{escape(_format_date(item.get('date_paiement')))}</td>\n"
            f'            <td><span class="type-tag {tag_class}">{type_label}</span></td>\n'
            f"            <td>{escape(str(item.get('reference') or ''))}</td>\n"
            f"            <td>{escape(str(item.get('entite_nom') or ''))}</td>\n"
            f"            <td>{escape(str(item.get('categorie') or ''))}</td>\n"
            f"            <td class=\"amount\">{_format_money(item.get('montant_ht'))} €</td>\n"
            f"            <td class=\"amount\">{_format_money(item.get('montant_tva'))} €</td>\n"
            f"            <td class=\"amount\"><strong>{_format_money(item.get('montant_ttc'))} €</strong></td>\n"
            "          </tr>"
        )

    if rows:
        return "\n".join(rows)

    return (
        '          <tr>\n'
        '            <td colspan="8" style="padding: 20px; text-align: center; color: #666;">'
        "Aucune facture payée sur cette période."
        "</td>\n"
        "          </tr>"
    )


def generate_registre_files(
    document_type: str,
    document_label: str,
    date_debut,
    date_fin,
    items,
    totals: dict,
    file_prefix: str,
):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    template_path = os.path.join(
        base_dir, "files", "templates", "template_registres.html"
    )
    output_dir = os.path.join(base_dir, "files", "registres")
    os.makedirs(output_dir, exist_ok=True)

    start_slug = date_debut.strftime("%Y-%m-%d")
    end_slug = date_fin.strftime("%Y-%m-%d")
    output_name = f"{file_prefix}_{start_slug}_{end_slug}"
    html_output_path = os.path.join(output_dir, f"{output_name}.html")
    pdf_output_path = os.path.join(output_dir, f"{output_name}.pdf")

    with open(template_path, "r", encoding="utf-8") as f:
        html_content = f.read()

    if document_type == "registre_achats":
        html_content = _remove_marked_block(html_content, "stat-card-ventes")
        html_content = _remove_marked_block(html_content, "stat-card-resultat")
    elif document_type == "registre_ventes":
        html_content = _remove_marked_block(html_content, "stat-card-achats")
        html_content = _remove_marked_block(html_content, "stat-card-resultat")

    generation_date = datetime.now().strftime("%d/%m/%Y")
    replacements = {
        "#date_document": generation_date,
        "#date_generation": generation_date,
        "#date_debut": _format_date(date_debut),
        "#date_fin": _format_date(date_fin),
        "#type_document": document_label,
        "#total_vente_ttc": _format_money(totals.get("vente_ttc")),
        "#total_vente_ht": _format_money(totals.get("vente_ht")),
        "#total_ventes_ttc": _format_money(totals.get("vente_ttc")),
        "#total_ventes_ht": _format_money(totals.get("vente_ht")),
        "#total_achat_ttc": _format_money(totals.get("achat_ttc")),
        "#total_achat_ht": _format_money(totals.get("achat_ht")),
        "#total_achats_ttc": _format_money(totals.get("achat_ttc")),
        "#total_achats_ht": _format_money(totals.get("achat_ht")),
        "#resultat_net_ht": _format_money(totals.get("resultat_ht")),
        "#resultat_net_ttc": _format_money(totals.get("resultat_ttc")),
        "#resultat_ttc": _format_money(totals.get("resultat_ttc")),
    }
    for key, value in replacements.items():
        html_content = html_content.replace(key, value)

    rows_html = _build_item_rows(items)
    html_content = re.sub(
        r"(<tbody>).*?(</tbody>)",
        lambda match: f"{match.group(1)}\n{rows_html}\n        {match.group(2)}",
        html_content,
        count=1,
        flags=re.DOTALL,
    )

    with open(html_output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    if os.path.exists(pdf_output_path):
        os.remove(pdf_output_path)

    options = {
        "enable-local-file-access": None,
        "encoding": "UTF-8",
        "page-size": "A4",
        "margin-top": "0mm",
        "margin-right": "0mm",
        "margin-bottom": "0mm",
        "margin-left": "0mm",
    }
    pdfkit.from_file(html_output_path, pdf_output_path, options=options)

    return {
        "html_path": html_output_path,
        "pdf_path": pdf_output_path,
        "url_path": f"/files/registres/{output_name}.pdf",
    }
