import datetime
import json
import os
import shutil
import sys
import urllib.request
from decimal import ROUND_HALF_UP, Decimal
from typing import List, Optional

from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    and_,
    create_engine,
    func,
    or_,
)
from sqlalchemy.orm import (
    Session,
    declarative_base,
    joinedload,
    relationship,
    sessionmaker,
)

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from scripts.generate_devis import generate_devis_files
from scripts.generate_facture import generate_facture_files

# --- Configurations de la Base de Données ---
# Par défaut, utilise SQLite en local pour faciliter les tests avec Pixi
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./crm_local.db")

if DATABASE_URL.startswith("sqlite"):
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
else:
    engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# --- Modèles SQLAlchemy ---
class Contact(Base):
    __tablename__ = "contacts"
    id = Column(Integer, primary_key=True, index=True)
    prenom = Column(String(100))
    nom = Column(String(100))
    entreprise = Column(String(150))
    siret = Column(String(14))
    tva_intra = Column(String(20))
    type_entite = Column(String(20), default="B2B")
    poste = Column(String(150))
    adresse_livraison = Column(Text)
    adresse_facturation = Column(Text)
    email = Column(String(255), unique=True, index=True, nullable=False)
    telephone = Column(String(30))
    date_creation = Column(DateTime(timezone=True), default=datetime.datetime.utcnow)

    requetes = relationship("Requete", back_populates="contact")
    devis = relationship("Devis", back_populates="contact")
    actions = relationship("Action", back_populates="contact")
    commandes = relationship("Commande", back_populates="contact")
    factures = relationship("Facture", back_populates="contact")


class Requete(Base):
    __tablename__ = "requetes"
    id = Column(Integer, primary_key=True, index=True)
    contact_id = Column(
        Integer, ForeignKey("contacts.id", ondelete="CASCADE"), nullable=False
    )
    priorite = Column(String(20), default="normale")
    sujet = Column(String(255))
    message = Column(Text, nullable=False)
    statut = Column(String(50), default="nouveau")
    source = Column(String(100), default="formulaire_web")
    date_reception = Column(DateTime(timezone=True), default=datetime.datetime.utcnow)

    contact = relationship("Contact", back_populates="requetes")


class Action(Base):
    __tablename__ = "actions"
    id = Column(Integer, primary_key=True, index=True)
    nom = Column(String(255), nullable=False)
    contact_id = Column(
        Integer, ForeignKey("contacts.id", ondelete="SET NULL"), nullable=True
    )
    detail = Column(Text)
    priorite = Column(String(50), default="normale")
    statut = Column(String(50), default="nouveau")
    date = Column(DateTime(timezone=True), default=datetime.datetime.utcnow)

    contact = relationship("Contact", back_populates="actions")


class Devis(Base):
    __tablename__ = "devis"
    id = Column(Integer, primary_key=True, index=True)
    nom = Column(String(255), nullable=False)
    client = Column(
        Integer, ForeignKey("contacts.id", ondelete="CASCADE"), nullable=False
    )
    description = Column(Text)
    montant_ht = Column(Numeric(10, 2))
    montant_tva = Column(Numeric(10, 2), default=0)
    montant_ttc = Column(Numeric(10, 2), default=0)
    file_path = Column(Text)
    statut = Column(String(50), default="En attente")
    type = Column(String(50), default="émis")
    date_emission = Column(DateTime(timezone=True), default=datetime.datetime.utcnow)

    contact = relationship("Contact", back_populates="devis")
    commandes = relationship("Commande", back_populates="devis")


class Commande(Base):
    __tablename__ = "commandes"
    id = Column(Integer, primary_key=True, index=True)
    reference = Column(String(100), unique=True)
    description = Column(Text)
    contact_id = Column(
        Integer, ForeignKey("contacts.id", ondelete="CASCADE"), nullable=False
    )
    devis_id = Column(
        Integer, ForeignKey("devis.id", ondelete="SET NULL"), nullable=True
    )
    flux = Column(String(20), nullable=False)  # 'vente' ou 'achat'
    statut = Column(String(50), default="en_attente")
    priorite = Column(String(20), default="normale")
    montant_ht = Column(Numeric(12, 2), default=0)
    montant_ttc = Column(Numeric(12, 2), default=0)
    file_path = Column(Text)
    date_commande = Column(DateTime(timezone=True), default=datetime.datetime.utcnow)
    date_livraison_prevue = Column(DateTime(timezone=True))
    url_suivi_colis = Column(Text)
    notes_internes = Column(Text)

    contact = relationship("Contact", back_populates="commandes")
    devis = relationship("Devis", back_populates="commandes")


class Facture(Base):
    __tablename__ = "factures"
    id = Column(Integer, primary_key=True, index=True)
    numero_facture = Column(String(50), unique=True, nullable=False)
    contact_id = Column(
        Integer, ForeignKey("contacts.id", ondelete="CASCADE"), nullable=False
    )
    devis_id = Column(
        Integer, ForeignKey("devis.id", ondelete="SET NULL"), nullable=True
    )
    commande_id = Column(
        Integer, ForeignKey("commandes.id", ondelete="SET NULL"), nullable=True
    )
    flux = Column(String(20), nullable=False)
    montant_ht = Column(Numeric(12, 2), nullable=False)
    montant_tva = Column(Numeric(12, 2), nullable=False)
    montant_ttc = Column(Numeric(12, 2), nullable=False)
    devise = Column(String(3), default="EUR")
    file_path = Column(Text)
    external_id = Column(String(255))
    statut_plateforme = Column(String(100), default="draft")
    statut_paiement = Column(String(50), default="non_paye")
    date_emission = Column(DateTime(timezone=True), default=datetime.datetime.utcnow)
    date_echeance = Column(DateTime(timezone=True))
    date_paiement = Column(DateTime(timezone=True))
    type_facture = Column("type", String(20), default="Facture")
    id_facture_associee = Column(
        Integer, ForeignKey("factures.id", ondelete="SET NULL"), nullable=True
    )
    montant_paye = Column(Numeric(12, 2), default=0)

    contact = relationship("Contact", back_populates="factures")
    devis = relationship("Devis", foreign_keys=[devis_id])
    commande = relationship("Commande", foreign_keys=[commande_id])
    facture_associee = relationship(
        "Facture",
        remote_side=[id],
        foreign_keys=[id_facture_associee],
        uselist=False,
    )


def _facture_platform_bucket(statut_plateforme: Optional[str]) -> str:
    s = (statut_plateforme or "").strip().lower() or "draft"
    if s == "draft":
        return "draft"
    if s in ("pending", "sent"):
        return "pending"
    if s == "validated":
        return "validated"
    if s == "rejected":
        return "rejected"
    return "pending"


def _post_facture_email_webhook(facture: Facture) -> None:
    contact = facture.contact
    payload = {
        "prenom": (contact.prenom if contact else "") or "",
        "email": (contact.email if contact else "") or "",
        "nom_facture": facture.numero_facture,
        "file_path": facture.file_path or "",
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        "https://n8n.mrliw.fr/webhook-test/envoi_factures",
        data=data,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=120) as response:
        response.read()


def _money_dec(value) -> Decimal:
    if value is None:
        return Decimal("0")
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _statut_paiement_from_montants(montant_paye: Decimal, montant_ttc: Decimal) -> str:
    if montant_ttc <= 0:
        return "non_paye"
    if montant_paye <= 0:
        return "non_paye"
    if montant_paye >= montant_ttc:
        return "paye"
    return "partiel"


def _facture_reste_montant_clause():
    """
    Reste à couvrir au sens montants (montant_paye < TTC si TTC > 0).
    Inclut les lignes historiques « paye » avec montant_paye < TTC (incohérence avant versements).
    """
    ttc = func.coalesce(Facture.montant_ttc, 0)
    pay = func.coalesce(Facture.montant_paye, 0)
    return or_(ttc <= 0, pay < ttc)


def _sync_facture_paiement_from_montants(facture: Facture, db: Session) -> None:
    """Aligne statut_paiement et date_paiement sur les montants si la ligne est incohérente."""
    _reconcile_paiement_rows([facture], db)


def _reconcile_paiement_rows(rows: List[Facture], db: Session) -> None:
    """Met à jour en base les statuts incohérents (ex. paye avec montant_paye < TTC)."""
    if not rows:
        return
    now = datetime.datetime.now(datetime.timezone.utc)
    changed = False
    for facture in rows:
        ttc = _money_dec(facture.montant_ttc)
        pay = _money_dec(facture.montant_paye)
        expected = _statut_paiement_from_montants(pay, ttc)
        if (facture.statut_paiement or "") == expected:
            continue
        facture.statut_paiement = expected
        if expected == "paye" and ttc > 0:
            if not facture.date_paiement:
                facture.date_paiement = now
        else:
            facture.date_paiement = None
        changed = True
    if not changed:
        return
    try:
        db.commit()
        for facture in rows:
            db.refresh(facture)
    except Exception:
        db.rollback()


# --- Schémas Pydantic ---
class ContactSchema(BaseModel):
    id: int
    prenom: Optional[str] = None
    nom: Optional[str] = None
    entreprise: Optional[str] = None
    siret: Optional[str] = None
    tva_intra: Optional[str] = None
    type_entite: Optional[str] = "B2B"
    poste: Optional[str] = None
    adresse_livraison: Optional[str] = None
    adresse_facturation: Optional[str] = None
    email: str
    telephone: Optional[str] = None
    date_creation: Optional[datetime.datetime] = None

    class Config:
        from_attributes = True


class ContactCreate(BaseModel):
    prenom: Optional[str] = None
    nom: Optional[str] = None
    entreprise: Optional[str] = None
    siret: Optional[str] = None
    tva_intra: Optional[str] = None
    type_entite: Optional[str] = "B2B"
    poste: Optional[str] = None
    email: str
    telephone: Optional[str] = None
    adresse_livraison: Optional[str] = None
    adresse_facturation: Optional[str] = None


class RequeteSchema(BaseModel):
    id: int
    contact_id: int
    priorite: Optional[str] = "normale"
    sujet: Optional[str] = None
    message: str
    statut: Optional[str] = "nouveau"
    source: Optional[str] = "formulaire_web"
    date_reception: Optional[datetime.datetime] = None
    contact: ContactSchema

    class Config:
        from_attributes = True


class StatutUpdate(BaseModel):
    statut: str


class ActionSchema(BaseModel):
    id: int
    nom: str
    contact_id: Optional[int] = None
    detail: Optional[str] = None
    priorite: Optional[str] = "normale"
    statut: Optional[str] = "nouveau"
    date: Optional[datetime.datetime] = None

    contact: Optional[ContactSchema] = None

    class Config:
        from_attributes = True


class ActionStatutUpdate(BaseModel):
    statut: str


class DevisSchema(BaseModel):
    id: int
    nom: str
    client: int
    description: Optional[str] = None
    montant_ht: Optional[float] = None
    montant_tva: Optional[float] = 0.0
    montant_ttc: Optional[float] = 0.0
    file_path: Optional[str] = None
    statut: Optional[str] = "En attente"
    type: Optional[str] = "émis"
    date_emission: Optional[datetime.datetime] = None

    # Relationship for frontend mapping
    contact: Optional[ContactSchema] = None

    class Config:
        from_attributes = True


class DevisStatutUpdate(BaseModel):
    statut: str


class CommandeSchema(BaseModel):
    id: int
    reference: Optional[str] = None
    description: Optional[str] = None
    contact_id: int
    devis_id: Optional[int] = None
    flux: str
    statut: Optional[str] = "en_attente"
    priorite: Optional[str] = "normale"
    montant_ht: Optional[float] = 0
    montant_ttc: Optional[float] = 0
    file_path: Optional[str] = None
    date_commande: Optional[datetime.datetime] = None
    date_livraison_prevue: Optional[datetime.datetime] = None
    url_suivi_colis: Optional[str] = None
    notes_internes: Optional[str] = None

    contact: Optional[ContactSchema] = None
    devis: Optional[DevisSchema] = None

    class Config:
        from_attributes = True


class StatutCommandeUpdate(BaseModel):
    statut: str


class CommandeUpdate(BaseModel):
    reference: str
    description: Optional[str] = None
    contact_id: int
    devis_id: Optional[int] = None
    priorite: str
    date_livraison_prevue: Optional[str] = None
    url_suivi_colis: Optional[str] = None
    notes_internes: Optional[str] = None


class FactureDevisMini(BaseModel):
    id: int
    nom: Optional[str] = None
    statut: Optional[str] = None

    class Config:
        from_attributes = True


class FactureCommandeMini(BaseModel):
    id: int
    reference: Optional[str] = None

    class Config:
        from_attributes = True


class FactureLieeMini(BaseModel):
    id: int
    numero_facture: Optional[str] = None

    class Config:
        from_attributes = True


class FactureSchema(BaseModel):
    id: int
    numero_facture: str
    contact_id: int
    devis_id: Optional[int] = None
    commande_id: Optional[int] = None
    flux: str
    montant_ht: Optional[float] = None
    montant_tva: Optional[float] = None
    montant_ttc: Optional[float] = None
    devise: Optional[str] = "EUR"
    file_path: Optional[str] = None
    external_id: Optional[str] = None
    statut_plateforme: Optional[str] = "draft"
    statut_paiement: Optional[str] = "non_paye"
    type_facture: Optional[str] = "Facture"
    montant_paye: Optional[float] = 0
    id_facture_associee: Optional[int] = None
    date_emission: Optional[datetime.datetime] = None
    date_echeance: Optional[datetime.datetime] = None
    date_paiement: Optional[datetime.datetime] = None
    contact: Optional[ContactSchema] = None
    devis: Optional[FactureDevisMini] = None
    commande: Optional[FactureCommandeMini] = None
    facture_associee: Optional[FactureLieeMini] = None

    class Config:
        from_attributes = True


class FactureVersementBody(BaseModel):
    montant: float


class FacturePlateformeUpdate(BaseModel):
    statut_plateforme: str
    envoyer_mail: bool = False


# --- Initialisation FastAPI ---
app = FastAPI(
    title="Dashboard CRM personnel", description="API pour CRM dashboard.mrliw.fr"
)


@app.on_event("startup")
def on_startup():
    # Crée les tables au démarrage (uniquement si elles n'existent pas déjà)
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# --- Routes de l'API ---
@app.get("/api/data", response_model=List[RequeteSchema])
def get_data(db: Session = Depends(get_db)):
    return db.query(Requete).all()


@app.patch("/api/requete/{requete_id}/statut", response_model=RequeteSchema)
def update_statut(
    requete_id: int, statut_update: StatutUpdate, db: Session = Depends(get_db)
):
    requete = db.query(Requete).filter(Requete.id == requete_id).first()
    if not requete:
        raise HTTPException(status_code=404, detail="Requête non trouvée")
    if statut_update.statut not in ["nouveau", "traite"]:
        raise HTTPException(status_code=400, detail="Statut invalide.")

    requete.statut = statut_update.statut
    db.commit()
    db.refresh(requete)
    return requete


@app.get("/api/contacts", response_model=List[ContactSchema])
def get_contacts(db: Session = Depends(get_db)):
    return db.query(Contact).all()


@app.post("/api/contacts", response_model=ContactSchema)
def create_contact(contact: ContactCreate, db: Session = Depends(get_db)):
    db_contact = db.query(Contact).filter(Contact.email == contact.email).first()
    if db_contact:
        raise HTTPException(status_code=400, detail="Le contact est déjà enregistré.")
    new_contact = Contact(
        prenom=contact.prenom,
        nom=contact.nom,
        entreprise=contact.entreprise,
        siret=contact.siret,
        tva_intra=contact.tva_intra,
        type_entite=contact.type_entite,
        poste=contact.poste,
        email=contact.email,
        telephone=contact.telephone,
        adresse_livraison=contact.adresse_livraison,
        adresse_facturation=contact.adresse_facturation,
    )
    db.add(new_contact)
    db.commit()
    db.refresh(new_contact)
    return new_contact


@app.put("/api/contacts/{contact_id}", response_model=ContactSchema)
def update_contact(
    contact_id: int, contact_update: ContactCreate, db: Session = Depends(get_db)
):
    contact = db.query(Contact).filter(Contact.id == contact_id).first()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact non trouvé")

    contact.prenom = contact_update.prenom
    contact.nom = contact_update.nom
    contact.entreprise = contact_update.entreprise
    contact.siret = contact_update.siret
    contact.tva_intra = contact_update.tva_intra
    contact.type_entite = contact_update.type_entite
    contact.poste = contact_update.poste
    contact.email = contact_update.email
    contact.telephone = contact_update.telephone
    contact.adresse_livraison = contact_update.adresse_livraison
    contact.adresse_facturation = contact_update.adresse_facturation

    db.commit()
    db.refresh(contact)
    return contact


@app.get("/api/actions", response_model=List[ActionSchema])
def get_actions(db: Session = Depends(get_db)):
    actions = db.query(Action).all()
    return [ActionSchema.from_orm(a) for a in actions]


@app.patch("/api/actions/{action_id}/statut", response_model=ActionSchema)
def update_action_statut(
    action_id: int, statut_update: ActionStatutUpdate, db: Session = Depends(get_db)
):
    action = db.query(Action).filter(Action.id == action_id).first()
    if not action:
        raise HTTPException(status_code=404, detail="Action non trouvée")

    action.statut = statut_update.statut
    db.commit()
    db.refresh(action)
    return action


@app.get("/api/commandes", response_model=List[CommandeSchema])
def get_commandes(db: Session = Depends(get_db)):
    return db.query(Commande).all()


@app.patch("/api/commandes/{commande_id}/statut", response_model=CommandeSchema)
def update_commande_statut(
    commande_id: int, statut_update: StatutCommandeUpdate, db: Session = Depends(get_db)
):
    commande = db.query(Commande).filter(Commande.id == commande_id).first()
    if not commande:
        raise HTTPException(status_code=404, detail="Commande non trouvée")

    commande.statut = statut_update.statut
    db.commit()
    db.refresh(commande)
    return commande


@app.patch("/api/commandes/{commande_id}", response_model=CommandeSchema)
def update_commande(
    commande_id: int, payload: CommandeUpdate, db: Session = Depends(get_db)
):
    commande = db.query(Commande).filter(Commande.id == commande_id).first()
    if not commande:
        raise HTTPException(status_code=404, detail="Commande non trouvée")

    contact = db.query(Contact).filter(Contact.id == payload.contact_id).first()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact introuvable")

    if payload.devis_id:
        devis = db.query(Devis).filter(Devis.id == payload.devis_id).first()
        if not devis:
            raise HTTPException(status_code=404, detail="Devis introuvable")

    commande.description = payload.description
    commande.contact_id = payload.contact_id
    commande.devis_id = payload.devis_id
    commande.priorite = payload.priorite
    commande.url_suivi_colis = payload.url_suivi_colis
    commande.notes_internes = payload.notes_internes

    if payload.date_livraison_prevue:
        try:
            commande.date_livraison_prevue = datetime.datetime.strptime(
                payload.date_livraison_prevue, "%Y-%m-%d"
            )
        except ValueError:
            pass
    else:
        commande.date_livraison_prevue = None

    db.commit()
    db.refresh(commande)
    return commande


@app.post("/api/commandes/manuel")
def create_manual_commande(
    reference: str = Form(...),
    description: str = Form(""),
    contact_id: int = Form(...),
    devis_id: Optional[int] = Form(None),
    priorite: str = Form("normale"),
    montant_ht: float = Form(0.0),
    montant_ttc: float = Form(0.0),
    date_livraison_prevue: str = Form(""),
    url_suivi_colis: str = Form(""),
    notes_internes: str = Form(""),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    contact = db.query(Contact).filter(Contact.id == contact_id).first()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact introuvable")

    if devis_id:
        devis = db.query(Devis).filter(Devis.id == devis_id).first()
        if not devis:
            raise HTTPException(status_code=404, detail="Devis introuvable")

    os.makedirs("./files/commandes", exist_ok=True)
    filename = file.filename
    safe_ref = (
        "".join([c if c.isalnum() else "_" for c in reference])
        if reference
        else "manuel"
    )
    import time

    unix_time = str(int(time.time()))
    safe_filename = f"{safe_ref}_{unix_time}_{filename}"

    file_system_path = f"./files/commandes/{safe_filename}"
    file_path = f"/files/commandes/{safe_filename}"

    try:
        with open(file_system_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur système fichier: {e}")

    dlp = None
    if date_livraison_prevue:
        try:
            dlp = datetime.datetime.strptime(date_livraison_prevue, "%Y-%m-%d")
        except ValueError:
            pass

    commande = Commande(
        reference=reference,
        description=description,
        contact_id=contact_id,
        devis_id=devis_id,
        flux="achat",
        statut="en_attente",
        priorite=priorite,
        montant_ht=montant_ht,
        montant_ttc=montant_ttc,
        file_path=file_path,
        date_livraison_prevue=dlp,
        url_suivi_colis=url_suivi_colis,
        notes_internes=notes_internes,
    )
    db.add(commande)
    db.commit()
    db.refresh(commande)
    return {"status": "success", "id": commande.id}


@app.get("/api/factures/stats-unpaid")
def factures_unpaid_stats(db: Session = Depends(get_db)):
    unpaid = (
        db.query(Facture)
        .filter(
            and_(
                _facture_reste_montant_clause(),
                func.lower(func.trim(func.coalesce(Facture.statut_plateforme, "")))
                != "rejected",
            )
        )
        .all()
    )
    out = {
        "total": len(unpaid),
        "draft": 0,
        "pending": 0,
        "validated": 0,
    }
    for f in unpaid:
        b = _facture_platform_bucket(f.statut_plateforme)
        if b in out:
            out[b] += 1

    pay_expr = func.coalesce(Facture.montant_paye, 0)
    ttc_expr = func.coalesce(Facture.montant_ttc, 0)
    rejected_expr = func.lower(func.trim(func.coalesce(Facture.statut_plateforme, "")))
    out["partiel"] = (
        db.query(Facture)
        .filter(
            and_(
                pay_expr > 0,
                pay_expr < ttc_expr,
                rejected_expr != "rejected",
            )
        )
        .count()
    )
    return out


@app.get("/api/factures", response_model=List[FactureSchema])
def list_factures(
    ref: Optional[str] = None,
    client: Optional[str] = None,
    include_paid: bool = True,
    db: Session = Depends(get_db),
):
    q = db.query(Facture).options(
        joinedload(Facture.contact),
        joinedload(Facture.devis),
        joinedload(Facture.commande),
        joinedload(Facture.facture_associee),
    )
    if not include_paid:
        q = q.filter(
            and_(
                _facture_reste_montant_clause(),
                func.lower(func.trim(func.coalesce(Facture.statut_plateforme, "")))
                != "rejected",
            )
        )
    if ref and ref.strip():
        term = f"%{ref.strip()}%"
        q = q.filter(
            or_(
                Facture.numero_facture.ilike(term),
                Facture.external_id.ilike(term),
            )
        )
    if client and client.strip():
        cterm = f"%{client.strip()}%"
        contact_ids = [
            row[0]
            for row in db.query(Contact.id)
            .filter(
                or_(
                    Contact.prenom.ilike(cterm),
                    Contact.nom.ilike(cterm),
                    Contact.entreprise.ilike(cterm),
                    Contact.email.ilike(cterm),
                )
            )
            .distinct()
            .all()
        ]
        if not contact_ids:
            return []
        q = q.filter(Facture.contact_id.in_(contact_ids))
    return q.order_by(
        Facture.date_echeance.is_(None),
        Facture.date_echeance.asc(),
        Facture.id.asc(),
    ).all()


@app.patch(
    "/api/factures/{facture_id}/statut-plateforme",
    response_model=FactureSchema,
)
def update_facture_statut_plateforme(
    facture_id: int,
    body: FacturePlateformeUpdate,
    db: Session = Depends(get_db),
):
    raw = (body.statut_plateforme or "").strip().lower()
    if raw not in ("validated", "rejected", "sent"):
        raise HTTPException(
            status_code=400,
            detail="statut_plateforme doit être « validated », « rejected » ou « sent ».",
        )
    facture = (
        db.query(Facture)
        .options(
            joinedload(Facture.contact),
            joinedload(Facture.devis),
            joinedload(Facture.commande),
            joinedload(Facture.facture_associee),
        )
        .filter(Facture.id == facture_id)
        .first()
    )
    if not facture:
        raise HTTPException(status_code=404, detail="Facture non trouvée")
    bucket = _facture_platform_bucket(facture.statut_plateforme)
    if raw in ("validated", "rejected"):
        if bucket != "draft":
            raise HTTPException(
                status_code=400,
                detail="Seules les factures en brouillon peuvent être validées ou rejetées.",
            )
    else:  # sent
        if bucket != "validated":
            raise HTTPException(
                status_code=400,
                detail="Seules les factures validées sur la plateforme peuvent être marquées comme envoyées.",
            )
        if body.envoyer_mail:
            if not facture.contact or not (facture.contact.email or "").strip():
                raise HTTPException(
                    status_code=400,
                    detail="Impossible d'envoyer la facture par mail : email client manquant.",
                )
            if not (facture.file_path or "").strip():
                raise HTTPException(
                    status_code=400,
                    detail="Impossible d'envoyer la facture par mail : fichier facture manquant.",
                )
            try:
                _post_facture_email_webhook(facture)
            except Exception as e:
                raise HTTPException(
                    status_code=502,
                    detail=f"Erreur lors de l'envoi mail via n8n : {e}",
                )
    facture.statut_plateforme = raw[:100]
    db.commit()
    db.refresh(facture)
    return facture


@app.head("/api/factures")
def head_list_factures():
    """Sondes HEAD (healthcheck Docker, proxy) : pas de corps, évite 405 sur GET /api/factures."""
    return Response(status_code=200)


@app.get("/api/factures/{facture_id}", response_model=FactureSchema)
def get_facture(facture_id: int, db: Session = Depends(get_db)):
    facture = (
        db.query(Facture)
        .options(
            joinedload(Facture.contact),
            joinedload(Facture.devis),
            joinedload(Facture.commande),
            joinedload(Facture.facture_associee),
        )
        .filter(Facture.id == facture_id)
        .first()
    )
    if not facture:
        raise HTTPException(status_code=404, detail="Facture non trouvée")
    _sync_facture_paiement_from_montants(facture, db)
    return facture


@app.patch("/api/factures/{facture_id}/versement", response_model=FactureSchema)
def add_facture_versement(
    facture_id: int,
    body: FactureVersementBody,
    db: Session = Depends(get_db),
):
    ajout = _money_dec(body.montant)
    if ajout <= 0:
        raise HTTPException(
            status_code=400,
            detail="Le montant du versement doit être strictement positif.",
        )
    facture = (
        db.query(Facture)
        .options(
            joinedload(Facture.contact),
            joinedload(Facture.devis),
            joinedload(Facture.commande),
            joinedload(Facture.facture_associee),
        )
        .filter(Facture.id == facture_id)
        .first()
    )
    if not facture:
        raise HTTPException(status_code=404, detail="Facture non trouvée")

    plat = (facture.statut_plateforme or "").strip().lower()
    if plat != "sent":
        raise HTTPException(
            status_code=400,
            detail="Les versements ne sont possibles qu'une fois la facture envoyée sur la plateforme (statut « sent »).",
        )

    ttc = _money_dec(facture.montant_ttc)
    pay = _money_dec(facture.montant_paye)
    if ttc <= 0:
        raise HTTPException(
            status_code=400,
            detail="Versement impossible : montant TTC nul ou absent.",
        )
    if pay >= ttc:
        raise HTTPException(
            status_code=400,
            detail="La facture est déjà entièrement payée.",
        )
    new_pay = (pay + ajout).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if new_pay > ttc:
        new_pay = ttc
    if new_pay <= pay:
        raise HTTPException(
            status_code=400,
            detail="Le versement n'augmente pas le montant payé (plafond TTC atteint).",
        )

    facture.montant_paye = new_pay
    st = _statut_paiement_from_montants(new_pay, ttc)
    facture.statut_paiement = st
    now = datetime.datetime.now(datetime.timezone.utc)
    if st == "paye":
        if not facture.date_paiement:
            facture.date_paiement = now
    else:
        facture.date_paiement = None

    db.commit()
    db.refresh(facture)
    return facture


@app.post("/api/factures/manuel")
def create_manual_facture(
    contact_id: int = Form(...),
    flux: str = Form("envoyée"),
    montant_ht: float = Form(...),
    montant_tva: float = Form(0.0),
    montant_ttc: float = Form(...),
    numero_facture: str = Form(""),
    devise: str = Form("EUR"),
    external_id: str = Form(""),
    statut_plateforme: str = Form("draft"),
    devis_id: str = Form(""),
    commande_id: str = Form(""),
    date_emission: str = Form(""),
    date_echeance: str = Form(""),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    contact = db.query(Contact).filter(Contact.id == contact_id).first()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact introuvable")

    flux_clean = (flux or "").strip()
    if flux_clean in ("envoyee", "envoyée"):
        flux_db = "envoyée"
    elif flux_clean in ("receptionnee", "réceptionnée"):
        flux_db = "réceptionnée"
    else:
        raise HTTPException(
            status_code=400,
            detail="flux doit être « envoyée » ou « réceptionnée ».",
        )

    d_id: Optional[int] = None
    if devis_id and str(devis_id).strip():
        try:
            d_id = int(devis_id)
        except ValueError:
            d_id = None
        if d_id is not None:
            if not db.query(Devis).filter(Devis.id == d_id).first():
                raise HTTPException(status_code=404, detail="Devis introuvable")

    c_id: Optional[int] = None
    if commande_id and str(commande_id).strip():
        try:
            c_id = int(commande_id)
        except ValueError:
            c_id = None
        if c_id is not None:
            if not db.query(Commande).filter(Commande.id == c_id).first():
                raise HTTPException(status_code=404, detail="Commande introuvable")

    def _parse_day(s: str) -> Optional[datetime.datetime]:
        if not s or not str(s).strip():
            return None
        try:
            d = datetime.datetime.strptime(str(s).strip()[:10], "%Y-%m-%d")
            return d.replace(tzinfo=datetime.timezone.utc)
        except ValueError:
            return None

    emission = _parse_day(date_emission) or datetime.datetime.now(datetime.timezone.utc)
    echeance = _parse_day(date_echeance)

    import time

    os.makedirs("./files/factures", exist_ok=True)
    filename = file.filename or "facture.pdf"
    safe_stem = (
        "".join(
            c if c.isalnum() or c in ("-", "_", ".") else "_"
            for c in (numero_facture or "facture")
        ).strip("_")
        or "facture"
    )
    if len(safe_stem) > 40:
        safe_stem = safe_stem[:40]
    unix_time = str(int(time.time()))
    safe_filename = f"{safe_stem}_{unix_time}_{filename}"
    file_system_path = f"./files/factures/{safe_filename}"
    web_path = f"/files/factures/{safe_filename}"

    try:
        with open(file_system_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur écriture fichier: {e}")

    base_num = (numero_facture or "").strip()
    if not base_num:
        if "." in filename:
            base_num = filename.rsplit(".", 1)[0]
        else:
            base_num = filename
        base_num = base_num.strip() or f"FAC-{unix_time}"
    if len(base_num) > 50:
        base_num = base_num[:50]

    num = base_num
    n = 0
    while db.query(Facture).filter(Facture.numero_facture == num).first():
        n += 1
        suf = f"-{n}"
        num = (
            (base_num[: 50 - len(suf)] + suf)
            if len(base_num) + len(suf) > 50
            else base_num + suf
        )

    facture = Facture(
        numero_facture=num,
        contact_id=contact_id,
        devis_id=d_id,
        commande_id=c_id,
        flux=flux_db,
        montant_ht=montant_ht,
        montant_tva=montant_tva,
        montant_ttc=montant_ttc,
        devise=(devise or "EUR")[:3],
        file_path=web_path,
        external_id=(external_id or None) or None,
        statut_plateforme=(statut_plateforme or "draft")[:100],
        statut_paiement="non_paye",
        date_emission=emission,
        date_echeance=echeance,
        date_paiement=None,
        type_facture="Facture",
        montant_paye=0,
        id_facture_associee=None,
    )
    db.add(facture)
    try:
        db.commit()
    except Exception as e:
        db.rollback()
        try:
            if os.path.isfile(file_system_path):
                os.remove(file_system_path)
        except OSError:
            pass
        raise HTTPException(status_code=400, detail=str(e))
    db.refresh(facture)
    return {
        "status": "success",
        "id": facture.id,
        "numero_facture": facture.numero_facture,
    }


class WebhookPayload(BaseModel):
    texte: str


class ArticlePayload(BaseModel):
    designation: str
    quantite: int
    prix_unitaire: float
    remise: float = 0.0


class ConfirmDevisPayload(BaseModel):
    id: Optional[int] = None
    nom: Optional[str] = ""
    prenom: Optional[str] = ""
    entreprise: Optional[str] = ""
    siret: Optional[str] = ""
    tva_intra: Optional[str] = ""
    type_entite: Optional[str] = "B2B"
    adresse_facturation: Optional[str] = ""
    adresse_livraison: Optional[str] = ""
    email: str
    articles: List[ArticlePayload] = []
    total_estime: float = 0.0
    tva_percent: float = 0.0
    montant_tva: float = 0.0
    montant_ttc: float = 0.0
    delai: Optional[str] = ""
    envoi: int = 1
    note: Optional[str] = ""
    texte: str
    designation: Optional[str] = ""


class UpdateDevisPayload(BaseModel):
    texte: str


@app.post("/api/devis/{devis_id}/update_webhook")
def trigger_update_devis_webhook(
    devis_id: int, payload: UpdateDevisPayload, db: Session = Depends(get_db)
):
    devis = db.query(Devis).filter(Devis.id == devis_id).first()
    if not devis:
        raise HTTPException(status_code=404, detail="Devis non trouvé")

    try:
        webhook_data = {
            "ref_devis": devis.nom,
            "description_de_devis": devis.description,
            "texte_modification": payload.texte,
        }
        data = json.dumps(webhook_data).encode("utf-8")
        req = urllib.request.Request(
            "https://n8n.mrliw.fr/webhook/update_devis",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        response = urllib.request.urlopen(req, timeout=120)
        response_body = response.read().decode("utf-8")
        return json.loads(response_body) if response_body else {"status": "envoyé"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/actions/webhook")
def trigger_action_webhook(payload: WebhookPayload):
    try:
        data = json.dumps({"action": payload.texte}).encode("utf-8")
        req = urllib.request.Request(
            "https://n8n.mrliw.fr/webhook/dashboard-actions",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req) as response:
            return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _resolve_devis_associe_label(value: str, db: Session) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    if value.isdigit():
        devis = db.query(Devis).filter(Devis.id == int(value)).first()
        return devis.nom if devis else value
    devis = db.query(Devis).filter(Devis.nom == value).first()
    return devis.nom if devis else value


def _enrich_facture_webhook_response(
    data, db: Session, fallback_devis_id: Optional[int] = None
):
    if not isinstance(data, dict):
        return data
    assoc = (data.get("devis_associe") or "").strip()
    if assoc:
        data["devis_associe"] = _resolve_devis_associe_label(assoc, db)
    elif fallback_devis_id:
        devis = db.query(Devis).filter(Devis.id == fallback_devis_id).first()
        if devis:
            data["devis_associe"] = devis.nom
    return data


def _post_invoices_dashboard_webhook(payload: dict):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        "https://n8n.mrliw.fr/webhook/invoces_dashboard_request",
        data=data,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=120) as response:
        res_body = response.read().decode("utf-8")
        try:
            return json.loads(res_body)
        except json.JSONDecodeError:
            return {"status": "success", "raw": res_body}


@app.post("/api/factures/webhook")
def trigger_factures_webhook(payload: WebhookPayload, db: Session = Depends(get_db)):
    texte = (payload.texte or "").strip()
    if not texte:
        raise HTTPException(status_code=400, detail="Le texte ne peut pas être vide.")
    try:
        result = _post_invoices_dashboard_webhook({"texte": texte})
        return _enrich_facture_webhook_response(result, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class ConfirmFacturePayload(BaseModel):
    prenom: Optional[str] = ""
    nom: Optional[str] = ""
    entreprise: Optional[str] = ""
    email: str
    adresse_facturation: Optional[str] = ""
    description: Optional[str] = ""
    devis_associe: Optional[str] = ""
    articles: List[ArticlePayload] = []


def _next_facture_reference(db: Session) -> str:
    now = datetime.datetime.now()
    yy = now.strftime("%y")
    mm = now.strftime("%m")
    prefix = f"mrliw_f{yy}{mm}"
    rows = (
        db.query(Facture.numero_facture)
        .filter(Facture.numero_facture.like(f"{prefix}%"))
        .all()
    )
    max_xx = -1
    for (num,) in rows:
        if num and num.startswith(prefix) and len(num) >= len(prefix) + 2:
            try:
                max_xx = max(max_xx, int(num[-2:]))
            except ValueError:
                pass
    return f"{prefix}{max_xx + 1:02d}"


@app.post("/api/factures/confirm")
def confirm_facture_creation(
    payload: ConfirmFacturePayload, db: Session = Depends(get_db)
):
    email = (payload.email or "").strip()
    if not email:
        raise HTTPException(status_code=400, detail="Email client requis.")
    if not payload.articles:
        raise HTTPException(status_code=400, detail="Au moins un article est requis.")

    total_ht = Decimal("0")
    for a in payload.articles:
        pu = Decimal(str(a.prix_unitaire))
        q = Decimal(str(a.quantite))
        r = Decimal(str(a.remise or 0))
        total_ht += pu * q * (Decimal("1") - r / Decimal("100"))
    total_ht = total_ht.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    montant_tva = Decimal("0")
    montant_ttc = total_ht

    contact = db.query(Contact).filter(Contact.email == email).first()
    if not contact:
        contact = Contact(email=email)
        db.add(contact)
    contact.prenom = payload.prenom or ""
    contact.nom = payload.nom or ""
    contact.entreprise = payload.entreprise or ""
    if payload.adresse_facturation:
        contact.adresse_facturation = payload.adresse_facturation
    db.commit()
    db.refresh(contact)

    ref_facture = _next_facture_reference(db)
    entreprise = (payload.entreprise or "").strip()
    prenom_nom = f"{payload.prenom or ''} {payload.nom or ''}".strip()
    nom_client = (
        f"{prenom_nom} ({entreprise})" if entreprise else prenom_nom
    )

    ref_devis = (payload.devis_associe or "").strip()
    devis_id = None
    if ref_devis:
        devis = db.query(Devis).filter(Devis.nom == ref_devis).first()
        if devis:
            devis_id = devis.id

    generations = generate_facture_files(
        ref_facture=ref_facture,
        nom_client=nom_client,
        adresse_client=payload.adresse_facturation or "",
        contact_client=email,
        articles=payload.articles,
        total_ht=float(total_ht),
        ref_devis=ref_devis,
        description=payload.description or "",
    )

    now = datetime.datetime.now(datetime.timezone.utc)
    echeance = now + datetime.timedelta(days=30)
    file_path = generations.get("url_path") or f"/files/factures/{ref_facture}.pdf"

    facture = Facture(
        numero_facture=ref_facture,
        contact_id=contact.id,
        devis_id=devis_id,
        commande_id=None,
        flux="envoyée",
        montant_ht=total_ht,
        montant_tva=montant_tva,
        montant_ttc=montant_ttc,
        devise="EUR",
        file_path=file_path,
        external_id=None,
        statut_plateforme="draft",
        statut_paiement="non_paye",
        date_emission=now,
        date_echeance=echeance,
        date_paiement=None,
        type_facture="Facture",
        montant_paye=0,
        id_facture_associee=None,
    )
    db.add(facture)
    try:
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    db.refresh(facture)

    return {
        "status": "success",
        "id": facture.id,
        "numero_facture": facture.numero_facture,
        "file_path": facture.file_path,
    }


@app.post("/api/devis/{devis_id}/facture_webhook")
def trigger_devis_facture_webhook(
    devis_id: int, payload: WebhookPayload, db: Session = Depends(get_db)
):
    devis = db.query(Devis).filter(Devis.id == devis_id).first()
    if not devis:
        raise HTTPException(status_code=404, detail="Devis non trouvé")
    if devis.statut != "Signé":
        raise HTTPException(
            status_code=400,
            detail="Seul un devis signé peut générer une facture.",
        )
    texte = (payload.texte or "").strip()
    if not texte:
        raise HTTPException(status_code=400, detail="Le texte ne peut pas être vide.")
    try:
        result = _post_invoices_dashboard_webhook({"texte": texte, "devis_id": devis_id})
        return _enrich_facture_webhook_response(result, db, fallback_devis_id=devis_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- Frontend (Static files & Templates) ---
app.mount("/css", StaticFiles(directory="css"), name="css")
app.mount("/img", StaticFiles(directory="img"), name="img")
app.mount("/files", StaticFiles(directory="files"), name="files")
app.mount(
    "/app/files", StaticFiles(directory="files"), name="app_files"
)  # Rétro-compatibilité pour les tests
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={},  # Tu peux ajouter tes variables ici si besoin
    )


@app.get("/contacts", response_class=HTMLResponse)
def page_contacts(request: Request):
    return templates.TemplateResponse(request=request, name="contacts.html", context={})


@app.get("/actions", response_class=HTMLResponse)
def page_actions(request: Request):
    return templates.TemplateResponse(request=request, name="actions.html", context={})


@app.get("/commandes", response_class=HTMLResponse)
def page_commandes(request: Request):
    return templates.TemplateResponse(
        request=request, name="commandes.html", context={}
    )


@app.get("/factures", response_class=HTMLResponse)
def page_factures(request: Request):
    return templates.TemplateResponse(request=request, name="factures.html", context={})


@app.get("/api/devis", response_model=List[DevisSchema])
def get_devis_list(db: Session = Depends(get_db)):
    devis_all = db.query(Devis).all()
    return devis_all


@app.patch("/api/devis/{devis_id}/statut", response_model=DevisSchema)
def update_devis_statut(
    devis_id: int, statut_update: DevisStatutUpdate, db: Session = Depends(get_db)
):
    devis_item = db.query(Devis).filter(Devis.id == devis_id).first()
    if not devis_item:
        raise HTTPException(status_code=404, detail="Devis non trouvé")

    if devis_item.statut == "Signé":
        raise HTTPException(
            status_code=400, detail="Un devis signé ne peut pas être modifié"
        )

    devis_item.statut = statut_update.statut
    db.commit()
    db.refresh(devis_item)
    return devis_item


@app.post("/api/devis/{devis_id}/upload_signed")
def upload_signed_devis(
    devis_id: int, file: UploadFile = File(...), db: Session = Depends(get_db)
):
    devis_item = db.query(Devis).filter(Devis.id == devis_id).first()
    if not devis_item:
        raise HTTPException(status_code=404, detail="Devis non trouvé")
    if devis_item.statut == "Signé":
        raise HTTPException(status_code=400, detail="Ce devis est déjà signé")

    if devis_item.file_path and devis_item.file_path.startswith("/files/"):
        file_system_path = "." + devis_item.file_path
    else:
        file_system_path = f"./files/devis/{devis_item.nom}.pdf"

    try:
        with open(file_system_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur système fichier: {e}")

    devis_item.statut = "Signé"
    db.commit()
    db.refresh(devis_item)
    return {"status": "success", "file_path": devis_item.file_path}


@app.post("/api/devis/manuel")
def create_manual_devis(
    nom: str = Form(""),
    client: int = Form(...),
    type: str = Form("émis"),
    description: str = Form(""),
    montant_ht: float = Form(0.0),
    montant_tva: float = Form(0.0),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    contact = db.query(Contact).filter(Contact.id == client).first()
    if not contact:
        raise HTTPException(status_code=404, detail="Client introuvable")

    filename = file.filename
    safe_nom = "".join([c if c.isalnum() else "_" for c in nom]) if nom else "manuel"
    # To keep files unique and safe
    import time

    unix_time = str(int(time.time()))
    safe_filename = f"{safe_nom}_{unix_time}_{filename}"

    file_system_path = f"./files/devis/{safe_filename}"
    file_path = f"/files/devis/{safe_filename}"

    try:
        with open(file_system_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur système fichier: {e}")

    devis = Devis(
        nom=nom or f"Devis manuel ({safe_filename})",
        client=client,
        description=description,
        montant_ht=montant_ht,
        montant_tva=montant_tva,
        montant_ttc=montant_ht + montant_tva,
        type=type,
        statut="Signé" if type == "reçu" else "En attente",
        file_path=file_path,
    )
    db.add(devis)
    db.commit()
    db.refresh(devis)
    return {"status": "success", "id": devis.id}


@app.post("/api/devis/webhook")
def trigger_devis_webhook(payload: WebhookPayload):
    try:
        data = json.dumps({"devis_request": payload.texte}).encode("utf-8")
        req = urllib.request.Request(
            "https://n8n.mrliw.fr/webhook/devis_dashboard_request",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req) as response:
            res_body = response.read().decode("utf-8")
            try:
                return json.loads(res_body)
            except:
                return {"raw": res_body}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/devis/confirm")
def confirm_devis_creation(payload: ConfirmDevisPayload, db: Session = Depends(get_db)):
    try:
        contact = db.query(Contact).filter(Contact.email == payload.email).first()
        if not contact:
            contact = Contact(email=payload.email)
            db.add(contact)

        contact.prenom = payload.prenom
        contact.nom = payload.nom
        contact.entreprise = payload.entreprise
        contact.siret = payload.siret
        contact.tva_intra = payload.tva_intra
        contact.type_entite = payload.type_entite
        if payload.adresse_facturation:
            contact.adresse_facturation = payload.adresse_facturation
        if payload.adresse_livraison:
            contact.adresse_livraison = payload.adresse_livraison

        db.commit()
        db.refresh(contact)

        if payload.id:
            devis = db.query(Devis).filter(Devis.id == payload.id).first()
            if not devis:
                raise HTTPException(
                    status_code=404, detail="Devis non trouvé pour la mise à jour."
                )
            ref_devis = devis.nom
        else:
            now = datetime.datetime.now()
            month = now.strftime("%m")
            year = now.strftime("%y")
            count = (
                db.query(Devis).filter(Devis.nom.like(f"mrliw_d{month}{year}%")).count()
            )
            ref_devis = f"mrliw_d{month}{year}{count:02d}"

        nom_client = (
            f"{payload.prenom} {payload.nom} ({payload.entreprise})"
            if payload.entreprise
            else f"{payload.prenom} {payload.nom}"
        )
        contact_client = payload.email
        if contact.telephone:
            contact_client = f"{contact.telephone} | {payload.email}"

        generations = generate_devis_files(
            ref_devis=ref_devis,
            nom_client=nom_client,
            adresse_client=payload.adresse_livraison or "",
            contact_client=contact_client,
            articles=payload.articles,
            total_ht=payload.total_estime,
            total_tva=payload.montant_tva,
            total_ttc=payload.montant_ttc,
            delai=payload.delai,
            notes=payload.note,
        )

        desc_lines = ["Articles:"]
        for a in payload.articles:
            remise_str = f" - Remise: {a.remise}%" if a.remise > 0 else ""
            desc_lines.append(
                f"- {a.quantite}x {a.designation} ({a.prix_unitaire}€{remise_str})"
            )
        if payload.note:
            desc_lines.append(f"\nNote: {payload.note}")

        file_path = generations.get("url_path", generations["pdf_path"])
        if payload.id:
            devis.description = "\n".join(desc_lines)
            devis.montant_ht = payload.total_estime
            devis.montant_tva = payload.montant_tva
            devis.montant_ttc = payload.montant_ttc
            devis.file_path = file_path
        else:
            devis = Devis(
                nom=ref_devis,
                client=contact.id,
                description="\n".join(desc_lines),
                montant_ht=payload.total_estime,
                montant_tva=payload.montant_tva,
                montant_ttc=payload.montant_ttc,
                statut="En attente",
                file_path=file_path,
            )
            db.add(devis)

        db.commit()
        db.refresh(devis)
        # Webhook final vers N8N
        if payload.envoi == 1:
            try:
                webhook_payload = {
                    "prenom": payload.prenom,
                    "email": payload.email,
                    "nom_devis": ref_devis,
                    "designation_devis": payload.designation,
                    "file_path": devis.file_path,
                }
                data = json.dumps(webhook_payload).encode("utf-8")
                req = urllib.request.Request(
                    "https://n8n.mrliw.fr/webhook/envoi_devis",
                    data=data,
                    headers={"Content-Type": "application/json"},
                )
                urllib.request.urlopen(req)
            except Exception as webhook_err:
                print(f"Erreur webhook n8n: {webhook_err}")

        return {"status": "success", "devis_id": devis.id, "ref": ref_devis}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/devis", response_class=HTMLResponse)
def page_devis(request: Request):
    return templates.TemplateResponse(request=request, name="devis.html", context={})
