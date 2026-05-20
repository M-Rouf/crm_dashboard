#!/usr/bin/env python3
"""Crée un utilisateur CRM (hash bcrypt). Usage:
  python scripts/create_utilisateur.py --email user@example.com --password 'secret' \\
    --nom Nom --prenom Prenom --entreprise-id 1
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from main import Utilisateur
from tenant_auth import hash_password


def main():
    parser = argparse.ArgumentParser(description="Créer un utilisateur CRM")
    parser.add_argument("--email", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--nom", required=True)
    parser.add_argument("--prenom", required=True)
    parser.add_argument("--entreprise-id", type=int, default=1)
    parser.add_argument("--role", default="admin")
    args = parser.parse_args()

    database_url = os.getenv("DATABASE_URL", "sqlite:///./crm_local.db")
    engine = create_engine(
        database_url,
        connect_args={"check_same_thread": False}
        if database_url.startswith("sqlite")
        else {},
    )
    Session = sessionmaker(bind=engine)
    db = Session()
    try:
        existing = db.query(Utilisateur).filter(Utilisateur.email == args.email).first()
        if existing:
            print(f"Utilisateur déjà présent (id={existing.id}).")
            return 1
        user = Utilisateur(
            entreprise_id=args.entreprise_id,
            nom=args.nom,
            prenom=args.prenom,
            email=args.email.strip().lower(),
            mot_de_passe_hash=hash_password(args.password),
            role=args.role,
            actif=True,
        )
        db.add(user)
        db.commit()
        db.refresh(user)
        print(f"Utilisateur créé : id={user.id}, email={user.email}, entreprise_id={user.entreprise_id}")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
