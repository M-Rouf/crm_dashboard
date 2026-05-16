import os
from datetime import datetime
from typing import Optional

import pdfkit


def _format_money(amount: float) -> str:
    return f"{amount:.2f}"


def generate_avoir_files(
    ref_avoir: str,
    ref_facture: str,
    nom_client: str,
    adresse_client: str,
    contact_client: str,
    description_avoir: str,
    montant: float,
    date_facture: Optional[datetime],
):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    template_path = os.path.join(base_dir, "files", "templates", "template_avoirs.html")
    output_dir = os.path.join(base_dir, "files", "avoirs")
    os.makedirs(output_dir, exist_ok=True)

    html_output_path = os.path.join(output_dir, f"{ref_avoir}.html")
    pdf_output_path = os.path.join(output_dir, f"{ref_avoir}.pdf")

    with open(template_path, "r", encoding="utf-8") as f:
        html_content = f.read()

    replacements = {
        "#ref_avoir": ref_avoir,
        "#ref_facture": ref_facture,
        "#date_avoir": datetime.now().strftime("%d/%m/%Y"),
        "#nom_client": nom_client,
        "#adresse_client": adresse_client or "",
        "#contact_client": contact_client or "",
        "#description_avoir": description_avoir,
        "#Tot_HT": _format_money(montant),
        "#Tot_TTC": _format_money(montant),
        "#date_facture": date_facture.strftime("%d/%m/%Y") if date_facture else "",
    }
    for key, val in replacements.items():
        html_content = html_content.replace(key, str(val))

    logo_path = os.path.join(base_dir, "files", "templates", "logo_devis.png")
    html_content = html_content.replace('src="logo_devis.png"', f'src="{logo_path}"')

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
        print(f"Erreur PDF generation avoir: {e}")

    return {
        "html_path": html_output_path,
        "pdf_path": pdf_output_path,
        "url_path": f"/files/avoirs/{ref_avoir}.pdf",
    }
