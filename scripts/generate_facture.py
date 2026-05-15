import os
import re
from datetime import datetime, timedelta

import pdfkit


def _format_money(amount: float) -> str:
    return f"{amount:.2f} €"


def generate_facture_files(
    ref_facture: str,
    nom_client: str,
    adresse_client: str,
    contact_client: str,
    articles,
    total_ht: float,
    ref_devis: str = "",
    ref_commande: str = "",
    description: str = "",
    mode_reglement: str = "Au choix du client",
):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    template_path = os.path.join(
        base_dir, "files", "templates", "template_factures.html"
    )
    output_dir = os.path.join(base_dir, "files", "factures")
    os.makedirs(output_dir, exist_ok=True)

    html_output_path = os.path.join(output_dir, f"{ref_facture}.html")
    pdf_output_path = os.path.join(output_dir, f"{ref_facture}.pdf")

    with open(template_path, "r", encoding="utf-8") as f:
        html_content = f.read()

    date_now = datetime.now()
    date_facture_str = date_now.strftime("%d/%m/%Y")
    date_echeance_str = (date_now + timedelta(days=30)).strftime("%d/%m/%Y")
    total_ttc = float(total_ht)

    replacements = {
        "#ref_facture": ref_facture,
        "#nom_client": nom_client,
        "#adresse_client": adresse_client or "",
        "#contact_client": contact_client or "",
        "#date_facture": date_facture_str,
        "#date_echeance": date_echeance_str,
        "#mode_reglement": mode_reglement,
        "#Tot_HT": _format_money(total_ht),
        "#Tot_TTC": _format_money(total_ttc),
    }
    for key, val in replacements.items():
        html_content = html_content.replace(key, str(val))

    logo_path = os.path.join(base_dir, "files", "templates", "logo_devis.png")
    html_content = html_content.replace('src="logo_devis.png"', f'src="{logo_path}"')

    ref_devis = (ref_devis or "").strip()
    ref_commande = (ref_commande or "").strip()
    if ref_devis:
        html_content = html_content.replace("#ref_devis", ref_devis)
    else:
        html_content = html_content.replace("#ref_devis", "")
        html_content = re.sub(
            r"Devis\s*:\s*(?:<br\s*/>)?\s*",
            "",
            html_content,
            count=1,
            flags=re.IGNORECASE,
        )
    if ref_commande:
        html_content = html_content.replace("#ref_commande", ref_commande)
    else:
        html_content = html_content.replace("#ref_commande", "")
        html_content = re.sub(
            r"Commande\s*:\s*[^\n<]*",
            "",
            html_content,
            count=1,
            flags=re.IGNORECASE,
        )

    article_row_pattern = re.search(
        r"(<tr>\s*<td>.*?#Nom_article.*?</tr>)",
        html_content,
        re.DOTALL,
    )
    if article_row_pattern:
        row_template = article_row_pattern.group(1)
        articles_html = ""
        desc_global = (description or "").strip()
        for idx, art in enumerate(articles):
            if hasattr(art, "model_dump"):
                art = art.model_dump()
            elif hasattr(art, "dict"):
                art = art.dict()
            ht_price = float(art.get("prix_unitaire", 0) or 0)
            qty = int(art.get("quantite", 1) or 1)
            remise = float(art.get("remise", 0) or 0)
            total_art_ht = ht_price * qty * (1 - remise / 100)
            detail = desc_global if idx == 0 and desc_global else ""

            row_html = row_template
            row_html = row_html.replace("#Nom_article", str(art.get("designation", "")))
            row_html = row_html.replace("#nb_article", str(qty))
            row_html = row_html.replace("#UHT", _format_money(ht_price))
            row_html = row_html.replace(
                "#remise", f"{remise:g} %" if remise > 0 else "—"
            )
            row_html = row_html.replace("#THT", _format_money(total_art_ht))
            row_html = row_html.replace("#description_detaillee", detail)
            articles_html += row_html + "\n"
        html_content = html_content.replace(row_template, articles_html)

    with open(html_output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    options = {
        "enable-local-file-access": None,
        "encoding": "UTF-8",
        "page-size": "A4",
        "margin-top": "0mm",
        "margin-right": "0mm",
        "margin-bottom": "0mm",
        "margin-left": "0mm",
    }
    try:
        pdfkit.from_file(html_output_path, pdf_output_path, options=options)
    except Exception as e:
        print(f"Erreur PDF generation facture: {e}")

    return {
        "html_path": html_output_path,
        "pdf_path": pdf_output_path,
        "url_path": f"/files/factures/{ref_facture}.pdf",
    }
