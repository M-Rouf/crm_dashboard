import os
import pdfkit
import re
from datetime import datetime, timedelta

def generate_devis_files(ref_devis, nom_client, adresse_client, contact_client, articles, total_ht, total_tva=0, total_ttc=0, delai=""):
    # Chemin vers les fichiers
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    template_path = os.path.join(base_dir, "files", "templates", "template_devis.html")
    output_dir = os.path.join(base_dir, "files", "devis")
    os.makedirs(output_dir, exist_ok=True)
    
    html_output_path = os.path.join(output_dir, f"{ref_devis}.html")
    pdf_output_path = os.path.join(output_dir, f"{ref_devis}.pdf")

    # Lecture du template HTML
    with open(template_path, 'r', encoding='utf-8') as f:
        html_content = f.read()

    # Dates
    date_now = datetime.now()
    date_devis_str = date_now.strftime("%d/%m/%Y")
    date_offre = (date_now + timedelta(weeks=6)).strftime("%d/%m/%Y")
    
    # Remplacements de base
    replacements = {
        "#ref_devis": ref_devis,
        "#nom_client": nom_client,
        "#adresse_client": adresse_client,
        "#contact_client": contact_client,
        "#prix_total": f"{total_ht:.2f} €",
        "#date_devis": date_devis_str,
        "#durée_offre": date_offre,
        "#Tot_HT": f"{total_ht:.2f} €",
        "#Tot_TVA": f"{total_tva:.2f} €",
        "#Tot_TTC": f"{total_ttc:.2f} €",
        "#delai": delai
    }
    
    for key, val in replacements.items():
        html_content = html_content.replace(key, str(val))

    # Correction du logo
    logo_path = os.path.join(base_dir, "files", "templates", "logo_devis.png")
    html_content = html_content.replace('src="logo_devis.png"', f'src="{logo_path}"')

    # Tableau d'articles
    # Extraction de la structure: <tr> ... <td>#ID</td> ... </tr>
    article_row_pattern = re.search(r'(<tr>\s*<td>#ID</td>.*?</tr>)', html_content, re.DOTALL)
    if article_row_pattern:
        row_template = article_row_pattern.group(1)
        articles_html = ""
        for i, art in enumerate(articles, start=1):
            if hasattr(art, 'dict'):
                art = art.dict()
            ht_price = float(art.get('prix_unitaire', 0))
            qty = int(art.get('quantite', 1))
            remise = float(art.get('remise', 0))
            
            # Application de la remise : prix * qty * (1 - remise/100)
            total_art_ht = (ht_price * qty) * (1 - remise / 100)
            
            row_html = row_template
            row_html = row_html.replace("#ID", str(i))
            row_html = row_html.replace("#Nom_article", str(art.get('designation', '')))
            row_html = row_html.replace("#nb_article", str(qty))
            row_html = row_html.replace("#UHT", f"{ht_price:.2f} €")
            row_html = row_html.replace("#remise", f"{remise}%" if remise > 0 else "")
            row_html = row_html.replace("#THT", f"{total_art_ht:.2f} €")
            row_html = row_html.replace("#TTC", f"{total_art_ht:.2f} €")
            
            articles_html += row_html + "\n"
        
        html_content = html_content.replace(row_template, articles_html)

    # Sauvegarde HTML
    with open(html_output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)

    # Génération PDF
    options = {
        'enable-local-file-access': None,
        'encoding': 'UTF-8',
        'page-size': 'A4',
        'margin-top': '0mm',
        'margin-right': '0mm',
        'margin-bottom': '0mm',
        'margin-left': '0mm'
    }
    try:
        pdfkit.from_file(html_output_path, pdf_output_path, options=options)
    except Exception as e:
        print(f"Erreur PDF generation: {e}")

    return {
        "html_path": html_output_path,
        "pdf_path": pdf_output_path,
        "url_path": f"/files/devis/{ref_devis}.pdf"
    }
