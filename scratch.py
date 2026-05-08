import sys
import os

# Add dashboard to path
sys.path.append('/run/media/matthieurouffy/Data/entreprenariat/dashboard')
from scripts.generate_devis import generate_devis_files

res = generate_devis_files(
    ref_devis="test_devis_123",
    nom_client="Test Client",
    adresse_client="123 rue de test",
    contact_client="test@test.com",
    articles=[],
    total_ht=100.0,
    total_tva=20.0,
    total_ttc=120.0,
    delai="1 semaine",
    notes="Ligne 1\nLigne 2\nLigne 3"
)
print(res)
