"""Placeholders entreprise (#nom_usage, #logo, etc.) pour les PDF/HTML générés."""

import os
from pathlib import Path
from typing import Any, Optional


def resolve_logo_file_path(nom_usage: Optional[str], base_dir: str) -> Optional[str]:
    if not nom_usage:
        return None
    name = nom_usage.strip()
    if not name or "/" in name or "\\" in name or ".." in name:
        return None
    logos_dir = Path(base_dir) / "files" / "logos"
    for ext in (".png", ".jpg", ".jpeg"):
        path = logos_dir / f"{name}{ext}"
        if path.is_file():
            return str(path.resolve())
    return None


def _field(entreprise: Any, attr: str, default: str = "") -> str:
    if entreprise is None:
        return default
    val = getattr(entreprise, attr, None)
    if val is None:
        return default
    return str(val).strip()


def _html_multiline(text: str) -> str:
    if not text:
        return ""
    return (
        text.replace("\r\n", "<br>")
        .replace("\n", "<br>")
        .replace("\r", "<br>")
    )


def build_entreprise_replacements(entreprise: Any, base_dir: str) -> dict:
    nom_usage = _field(entreprise, "nom_usage")
    logo_path = resolve_logo_file_path(nom_usage, base_dir)
    if logo_path:
        logo_src = Path(logo_path).as_uri()
    else:
        fallback = Path(base_dir) / "files" / "templates" / "logo_devis.png"
        logo_src = fallback.resolve().as_uri() if fallback.is_file() else ""

    return {
        "#logo": logo_src,
        "#nom_usage": nom_usage,
        "#raison_sociale": _field(entreprise, "raison_sociale"),
        "#adresse_entreprise": _html_multiline(_field(entreprise, "adresse")),
        "#code_postal": _field(entreprise, "code_postal"),
        "#ville_entreprise": _field(entreprise, "ville"),
        "#siret_entreprise": _field(entreprise, "siret"),
        "#telephone_entreprise": _field(entreprise, "telephone"),
        "#mail_entreprise": _field(entreprise, "email_contact"),
        "#rib_entreprise": _field(entreprise, "rib"),
        "#bic_entreprise": _field(entreprise, "bic"),
    }


def apply_entreprise_placeholders(
    html_content: str, entreprise: Any, base_dir: Optional[str] = None
) -> str:
    if base_dir is None:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if entreprise is None:
        return html_content
    for key, value in build_entreprise_replacements(entreprise, base_dir).items():
        html_content = html_content.replace(key, value)
    return html_content
