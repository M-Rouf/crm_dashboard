import os
from datetime import datetime
from typing import Optional

import pdfkit

from scripts.entreprise_template import apply_entreprise_placeholders
from scripts.facturx_invoice import make_facturx_pdf


def _format_money(amount: float) -> str:
    return f"{amount:.2f}"


def _format_taux(taux: float) -> str:
    if taux == int(taux):
        return str(int(taux))
    return f"{taux:g}"


TVA_AVOIR_FRANCHISE = "Franchise en base de TVA, art. 293 B du CGI."
TVA_AVOIR_APPLICABLE = "TVA soumise au taux en vigueur."


def generate_avoir_files(
    ref_avoir: str,
    ref_facture: str,
    nom_client: str,
    adresse_client: str,
    contact_client: str,
    description_avoir: str,
    montant_ht: float,
    montant_tva: float,
    montant_ttc: float,
    taux_tva: float,
    date_facture: Optional[datetime],
    entreprise=None,
    buyer_entreprise: str = "",
    buyer_prenom: str = "",
    buyer_nom: str = "",
    buyer_siret: str = "",
    buyer_tva_intra: str = "",
    buyer_type_entite: str = "B2B",
    seller_electronic_address: str = "",
    facturx: bool = True,
):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    template_path = os.path.join(base_dir, "files", "templates", "template_avoirs.html")
    output_dir = os.path.join(base_dir, "files", "avoirs")
    os.makedirs(output_dir, exist_ok=True)

    html_output_path = os.path.join(output_dir, f"{ref_avoir}.html")
    pdf_output_path = os.path.join(output_dir, f"{ref_avoir}.pdf")

    with open(template_path, "r", encoding="utf-8") as f:
        html_content = f.read()

    taux = float(taux_tva or 0)
    tva = float(montant_tva or 0)
    ht = float(montant_ht or 0)
    ttc = float(montant_ttc or 0)
    tva_legal = TVA_AVOIR_FRANCHISE if taux <= 0 else TVA_AVOIR_APPLICABLE
    taux_str = _format_taux(taux)

    replacements = {
        "#ref_avoir": ref_avoir,
        "#ref_facture": ref_facture,
        "#date_avoir": datetime.now().strftime("%d/%m/%Y"),
        "#nom_client": nom_client,
        "#adresse_client": adresse_client or "",
        "#contact_client": contact_client or "",
        "#description_avoir": description_avoir,
        "#Tot_HT": _format_money(ht),
        "#Tot_TTC": _format_money(ttc),
        "#taux_tva": taux_str,
        "#taux_TVA": taux_str,
        "#tot_TVA": _format_money(tva),
        "#tva_applicable": tva_legal,
        "#date_facture": date_facture.strftime("%d/%m/%Y") if date_facture else "",
    }
    for key, val in replacements.items():
        html_content = html_content.replace(key, str(val))

    html_content = apply_entreprise_placeholders(html_content, entreprise, base_dir)

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

    result = {
        "html_path": html_output_path,
        "pdf_path": pdf_output_path,
        "url_path": f"/files/avoirs/{ref_avoir}.pdf",
        "facturx": False,
        "xml_path": None,
    }

    if facturx and os.path.isfile(pdf_output_path):
        tva_ok = float(taux_tva or 0) > 0
        articles = [
            {
                "designation": description_avoir or f"Avoir sur {ref_facture}",
                "quantite": 1,
                "prix_unitaire": abs(ht),
                "remise": 0,
            }
        ]
        fx = make_facturx_pdf(
            pdf_output_path,
            ref_facture=ref_avoir,
            articles=articles,
            total_ht=abs(ht),
            montant_tva=abs(tva),
            total_ttc=abs(ttc),
            tva_applicable=tva_ok,
            taux_tva=taux if tva_ok else 0.0,
            date_emission=datetime.now(),
            date_echeance=datetime.now(),
            entreprise=entreprise,
            nom_client=nom_client,
            adresse_client=adresse_client or "",
            email_client=contact_client or "",
            buyer_entreprise=buyer_entreprise,
            buyer_prenom=buyer_prenom,
            buyer_nom=buyer_nom,
            buyer_siret=buyer_siret,
            buyer_tva_intra=buyer_tva_intra,
            buyer_type_entite=buyer_type_entite,
            seller_electronic_address=seller_electronic_address,
            description=f"Avoir relatif à la facture {ref_facture}. {description_avoir or ''}".strip(),
            document_type_code="381",
            preceding_invoice_ref=ref_facture,
            preceding_invoice_date=date_facture,
        )
        result["facturx"] = bool(fx.get("facturx"))
        result["xml_path"] = fx.get("xml_path")
        if fx.get("error"):
            print(f"Erreur Factur-X avoir: {fx['error']}")

    return result
