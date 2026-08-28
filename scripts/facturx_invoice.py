"""Génération Factur-X (CII EN 16931) et embarquement dans le PDF."""

from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Optional

logger = logging.getLogger(__name__)

TVA_EXEMPT_REASON = "TVA non applicable, art. 293 B du CGI"


def _digits(value: Optional[str]) -> str:
    return "".join(c for c in (value or "") if c.isdigit())


def french_vat_from_siret(siret: Optional[str]) -> str:
    """Calcule le n° de TVA intracommunautaire FR à partir du SIRET (SIREN)."""
    digits = _digits(siret)
    if len(digits) < 9:
        return ""
    siren = digits[:9]
    key = (12 + 3 * (int(siren) % 97)) % 97
    return f"FR{key:02d}{siren}"


def _money_str(amount) -> str:
    try:
        return f"{float(amount):.2f}"
    except (TypeError, ValueError):
        return "0.00"


def _as_date(value, default: Optional[date] = None) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y%m%d"):
            try:
                return datetime.strptime(value.strip()[:10], fmt).date()
            except ValueError:
                continue
    return default or date.today()


def parse_fr_address(
    text: Optional[str],
    fallback_cp: str = "",
    fallback_ville: str = "",
) -> tuple[str, str, str]:
    """Extrait (ligne1, code_postal, ville) d'une adresse libre française."""
    raw = (text or "").strip()
    if not raw:
        return (
            "Non renseignée",
            (fallback_cp or "00000")[:10],
            (fallback_ville or "Non renseignée").strip() or "Non renseignée",
        )

    m = re.search(r"(?P<cp>\d{5})\s+(?P<ville>[^\n,]+)\s*$", raw, re.MULTILINE)
    if m:
        line1 = raw[: m.start()].strip().rstrip(",").strip()
        ville = m.group("ville").strip()
        return (
            line1 or raw,
            m.group("cp"),
            ville or (fallback_ville or "Non renseignée"),
        )

    lines = [ln.strip() for ln in re.split(r"[\n,]+", raw) if ln.strip()]
    line1 = lines[0] if lines else raw
    return (
        line1,
        (fallback_cp or "00000")[:10],
        (fallback_ville or "Non renseignée").strip() or "Non renseignée",
    )


def _article_as_dict(art: Any) -> dict:
    if hasattr(art, "model_dump"):
        return art.model_dump()
    if hasattr(art, "dict"):
        return art.dict()
    if isinstance(art, dict):
        return art
    return {}


def _seller_name(entreprise) -> str:
    if not entreprise:
        return "Vendeur"
    return (
        (getattr(entreprise, "raison_sociale", None) or "").strip()
        or (getattr(entreprise, "nom_usage", None) or "").strip()
        or "Vendeur"
    )


def _buyer_name(
    nom_client: str = "",
    buyer_entreprise: str = "",
    buyer_prenom: str = "",
    buyer_nom: str = "",
) -> str:
    ent = (buyer_entreprise or "").strip()
    if ent:
        return ent
    person = f"{(buyer_prenom or '').strip()} {(buyer_nom or '').strip()}".strip()
    if person:
        return person
    return (nom_client or "").strip() or "Client"


def build_facturx_data_dict(
    *,
    ref_facture: str,
    articles: Iterable,
    total_ht: float,
    montant_tva: float,
    total_ttc: Optional[float] = None,
    tva_applicable: bool = False,
    taux_tva: float = 0.0,
    date_emission=None,
    date_echeance=None,
    entreprise=None,
    nom_client: str = "",
    adresse_client: str = "",
    email_client: str = "",
    buyer_entreprise: str = "",
    buyer_prenom: str = "",
    buyer_nom: str = "",
    buyer_siret: str = "",
    buyer_tva_intra: str = "",
    buyer_type_entite: str = "B2B",
    seller_electronic_address: str = "",
    business_process_type: str = "S1",
    ref_devis: str = "",
    ref_commande: str = "",
    description: str = "",
    document_type_code: str = "380",
    preceding_invoice_ref: str = "",
    preceding_invoice_date=None,
) -> dict:
    """Construit le dictionnaire EN 16931 attendu par facturx.generate_cii_xml."""
    issue_date = _as_date(date_emission)
    due_date = _as_date(date_echeance, default=issue_date + timedelta(days=30))
    ht = float(total_ht or 0)
    tva = float(montant_tva or 0)
    ttc = float(total_ttc if total_ttc is not None else ht + tva)
    taux = float(taux_tva or 0) if tva_applicable else 0.0

    seller_siret = _digits(getattr(entreprise, "siret", None) if entreprise else None)
    seller_siren = seller_siret[:9] if len(seller_siret) >= 9 else ""
    seller_vat = ""
    if entreprise:
        seller_vat = (
            (getattr(entreprise, "tva_intra", None) or "").strip().upper().replace(" ", "")
        )

    seller_line, seller_cp, seller_ville = parse_fr_address(
        getattr(entreprise, "adresse", None) if entreprise else "",
        fallback_cp=(getattr(entreprise, "code_postal", None) or "") if entreprise else "",
        fallback_ville=(getattr(entreprise, "ville", None) or "") if entreprise else "",
    )
    if entreprise and (getattr(entreprise, "code_postal", None) or "").strip():
        seller_cp = str(entreprise.code_postal).strip()
    if entreprise and (getattr(entreprise, "ville", None) or "").strip():
        seller_ville = str(entreprise.ville).strip()

    buyer_line, buyer_cp, buyer_ville = parse_fr_address(adresse_client)
    buyer_siret_d = _digits(buyer_siret)
    buyer_siren = buyer_siret_d[:9] if len(buyer_siret_d) >= 9 else ""
    is_b2b = (buyer_type_entite or "B2B").strip().upper() == "B2B"
    buyer_vat = ""
    if is_b2b:
        buyer_vat = (buyer_tva_intra or "").strip().upper().replace(" ", "")

    data: dict = {
        "BT-1": (ref_facture or "").strip()[:50],
        "BT-2": issue_date,
        "BT-3": document_type_code,
        "BT-5": "EUR",
        "BT-8": "invoice",
        "BT-9": due_date,
        # BT-23 : type de processus métier (e-reporting FR)
        # S1 = prestation de services ; B1 = vente de biens
        "BT-23": (business_process_type or "S1").strip().upper() or "S1",
        "BT-27": _seller_name(entreprise),
        "BT-35": seller_line,
        "BT-37": seller_ville,
        "BT-38": seller_cp,
        "BT-40": "FR",
        "BT-44": _buyer_name(nom_client, buyer_entreprise, buyer_prenom, buyer_nom),
        "BT-50": buyer_line,
        "BT-52": buyer_ville,
        "BT-53": buyer_cp,
        "BT-55": "FR",
        # Livraison = date d'émission (services / facturation à la date)
        "BT-72": issue_date,
        "BT-70": _buyer_name(nom_client, buyer_entreprise, buyer_prenom, buyer_nom),
        "BT-75": buyer_line,
        "BT-77": buyer_ville,
        "BT-78": buyer_cp,
        "BT-80": "FR",
        "BT-106": _money_str(ht),
        "BT-109": _money_str(ht),
        "BT-110": _money_str(tva),
        "BT-110-1": "EUR",
        "BT-112": _money_str(ttc),
        "BT-115": _money_str(ttc),
        "BG-1": [],
        "BG-23": [],
        "BG-25": [],
    }

    if seller_siret:
        data["BT-29"] = {"0009": seller_siret}
    if seller_siren:
        data["BT-30"] = seller_siren
        data["BT-30-1"] = "0002"
    if seller_vat:
        data["BT-31"] = seller_vat

    seller_email = (seller_electronic_address or "").strip()
    if not seller_email and entreprise:
        seller_email = (getattr(entreprise, "email_contact", None) or "").strip()
    seller_phone = (getattr(entreprise, "telephone", None) or "").strip() if entreprise else ""
    # BT-34 = adresse électronique du vendeur (requis Super PDP / FE)
    # scheme EM = Electronic Mail (EAS code list)
    if seller_email:
        data["BT-34"] = seller_email
        data["BT-34-1"] = "EM"
        data["BT-43"] = seller_email
    if seller_phone:
        data["BT-42"] = seller_phone

    if buyer_siret_d:
        data["BT-46"] = {"0009": buyer_siret_d}
    if buyer_siren:
        data["BT-47"] = buyer_siren
        data["BT-47-1"] = "0002"
    if buyer_vat:
        data["BT-48"] = buyer_vat
    if (email_client or "").strip():
        data["BT-58"] = email_client.strip()

    if (ref_devis or "").strip():
        data["BT-14"] = ref_devis.strip()[:35]
    if (ref_commande or "").strip():
        data["BT-13"] = ref_commande.strip()[:35]
    if (preceding_invoice_ref or "").strip():
        prev = {"BT-25": preceding_invoice_ref.strip()[:35]}
        if preceding_invoice_date:
            prev["BT-26"] = _as_date(preceding_invoice_date)
        data["BG-3"] = [prev]

    note = (description or "").strip()
    if note:
        data["BG-1"].append({"BT-21": "AAI", "BT-22": note[:1000]})
    if not tva_applicable:
        data["BG-1"].append({"BT-21": "TXD", "BT-22": TVA_EXEMPT_REASON})

    if tva_applicable and taux > 0:
        data["BG-23"].append(
            {
                "BT-116": _money_str(ht),
                "BT-116-1": "EUR",
                "BT-117": _money_str(tva),
                "BT-117-1": "EUR",
                "BT-118": "S",
                "BT-119": f"{taux:.2f}",
            }
        )
        line_tax_cat = "S"
        line_tax_rate = f"{taux:.2f}"
    else:
        data["BG-23"].append(
            {
                "BT-116": _money_str(ht),
                "BT-116-1": "EUR",
                "BT-117": "0.00",
                "BT-117-1": "EUR",
                "BT-118": "E",
                "BT-120": TVA_EXEMPT_REASON,
            }
        )
        line_tax_cat = "E"
        line_tax_rate = None

    rib = (getattr(entreprise, "rib", None) or "").strip().replace(" ", "") if entreprise else ""
    bic = (getattr(entreprise, "bic", None) or "").strip().upper() if entreprise else ""
    if rib:
        try:
            from stdnum import iban as std_iban

            rib = std_iban.compact(rib)
            if not std_iban.is_valid(rib):
                rib = ""
        except Exception:
            rib = ""
    if rib:
        data["BT-81"] = "30"
        data["BT-84"] = rib
        data["BT-85"] = _seller_name(entreprise)
        if bic:
            data["BT-86"] = bic

    for idx, raw_art in enumerate(articles or [], start=1):
        art = _article_as_dict(raw_art)
        qty = max(1, int(float(art.get("quantite") or 1)))
        pu = float(art.get("prix_unitaire") or 0)
        remise = float(art.get("remise") or 0)
        line_ht = pu * qty * (1 - remise / 100.0)
        designation = (art.get("designation") or f"Article {idx}").strip() or f"Article {idx}"
        line: dict = {
            "BT-126": str(idx),
            "BT-153": designation[:100],
            "BT-129": str(qty),
            "BT-130": "C62",
            "BT-146": _money_str(pu * (1 - remise / 100.0) if remise else pu),
            "BT-131": _money_str(line_ht),
            "BT-151": line_tax_cat,
        }
        if line_tax_rate is not None:
            line["BT-152"] = line_tax_rate
        if remise > 0:
            line["BT-127"] = f"Remise {remise:g} %"
        data["BG-25"].append(line)

    if not data["BG-25"]:
        data["BG-25"].append(
            {
                "BT-126": "1",
                "BT-153": "Prestation",
                "BT-129": "1",
                "BT-130": "C62",
                "BT-146": _money_str(ht),
                "BT-131": _money_str(ht),
                "BT-151": line_tax_cat,
                **({"BT-152": line_tax_rate} if line_tax_rate is not None else {}),
            }
        )

    return data


def generate_facturx_xml(data_dict: dict, level: str = "en16931") -> bytes:
    from facturx import generate_cii_xml

    return generate_cii_xml(
        data_dict,
        level=level,
        check_xsd=True,
        check_schematron=False,
        prefixed_namespaces=True,
    )


def embed_facturx_in_pdf(
    pdf_path: str,
    xml_bytes: bytes,
    xml_output_path: Optional[str] = None,
    level: str = "en16931",
) -> bool:
    """Embarque le XML CII dans le PDF (Factur-X). Retourne True si OK."""
    if not pdf_path or not os.path.isfile(pdf_path):
        logger.warning("PDF introuvable pour Factur-X: %s", pdf_path)
        return False
    if xml_output_path:
        os.makedirs(os.path.dirname(xml_output_path) or ".", exist_ok=True)
        with open(xml_output_path, "wb") as f:
            f.write(xml_bytes)

    from facturx import generate_from_file

    generate_from_file(
        pdf_path,
        xml_bytes,
        flavor="factur-x",
        level=level,
        check_xsd=True,
        check_schematron=False,
        output_pdf_file=pdf_path,
    )
    return True


def make_facturx_pdf(
    pdf_path: str,
    *,
    ref_facture: str,
    articles,
    total_ht: float,
    montant_tva: float,
    total_ttc: Optional[float] = None,
    tva_applicable: bool = False,
    taux_tva: float = 0.0,
    date_emission=None,
    date_echeance=None,
    entreprise=None,
    nom_client: str = "",
    adresse_client: str = "",
    email_client: str = "",
    buyer_entreprise: str = "",
    buyer_prenom: str = "",
    buyer_nom: str = "",
    buyer_siret: str = "",
    buyer_tva_intra: str = "",
    buyer_type_entite: str = "B2B",
    seller_electronic_address: str = "",
    business_process_type: str = "S1",
    ref_devis: str = "",
    ref_commande: str = "",
    description: str = "",
    document_type_code: str = "380",
    preceding_invoice_ref: str = "",
    preceding_invoice_date=None,
    level: str = "en16931",
) -> dict:
    """
    Génère le XML Factur-X et l'intègre au PDF.
    Retourne {xml_path, facturx: bool, error?: str}.
    """
    xml_path = os.path.splitext(pdf_path)[0] + ".xml"
    try:
        data = build_facturx_data_dict(
            ref_facture=ref_facture,
            articles=articles,
            total_ht=total_ht,
            montant_tva=montant_tva,
            total_ttc=total_ttc,
            tva_applicable=tva_applicable,
            taux_tva=taux_tva,
            date_emission=date_emission,
            date_echeance=date_echeance,
            entreprise=entreprise,
            nom_client=nom_client,
            adresse_client=adresse_client,
            email_client=email_client,
            buyer_entreprise=buyer_entreprise,
            buyer_prenom=buyer_prenom,
            buyer_nom=buyer_nom,
            buyer_siret=buyer_siret,
            buyer_tva_intra=buyer_tva_intra,
            buyer_type_entite=buyer_type_entite,
            seller_electronic_address=seller_electronic_address,
            business_process_type=business_process_type,
            ref_devis=ref_devis,
            ref_commande=ref_commande,
            description=description,
            document_type_code=document_type_code,
            preceding_invoice_ref=preceding_invoice_ref,
            preceding_invoice_date=preceding_invoice_date,
        )
        xml_bytes = generate_facturx_xml(data, level=level)
        ok = embed_facturx_in_pdf(pdf_path, xml_bytes, xml_output_path=xml_path, level=level)
        return {"xml_path": xml_path, "facturx": ok}
    except Exception as e:
        logger.exception("Échec génération Factur-X pour %s", ref_facture)
        return {"xml_path": xml_path, "facturx": False, "error": str(e)}
