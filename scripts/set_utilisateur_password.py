#!/usr/bin/env python3
"""Met à jour le mot de passe d'un utilisateur existant."""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from main import Utilisateur
from tenant_auth import hash_password


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--email", required=True)
    parser.add_argument("--password", required=True)
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
        user = (
            db.query(Utilisateur)
            .filter(Utilisateur.email == args.email.strip().lower())
            .first()
        )
        if not user:
            print("Utilisateur introuvable.")
            return 1
        user.mot_de_passe_hash = hash_password(args.password)
        db.commit()
        print(f"Mot de passe mis à jour pour {user.email} (id={user.id}).")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
