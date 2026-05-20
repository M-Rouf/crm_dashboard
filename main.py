import calendar
import datetime
import json
import os
import shutil
import sys
import urllib.request
from decimal import ROUND_HALF_UP, Decimal
from typing import Dict, List, Optional

from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    and_,
    cast,
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
from scripts.generate_avoir import generate_avoir_files
from scripts.generate_facture import generate_facture_files
from scripts.generate_registre import generate_registre_files
from tenant_auth import (
    get_one,
    get_session_user,
    hash_password,
    is_primary_user,
    require_admin,
    require_primary_user,
    scoped,
    setup_auth_middleware,
    setup_session_middleware,
    verify_password,
    webhook_entreprise_id,
)

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
class Entreprise(Base):
    __tablename__ = "entreprises"
    id = Column(Integer, primary_key=True, index=True)
    nom_usage = Column(String(100), nullable=False)
    raison_sociale = Column(String(150))
    siret = Column(String(14), unique=True, nullable=False)
    adresse = Column(Text)
    code_postal = Column(String(10))
    ville = Column(String(100))
    telephone = Column(String(20))
    email_contact = Column(String(100))
    id_super_pdp = Column(String(100))
    date_creation = Column(DateTime(timezone=True), default=datetime.datetime.utcnow)


class Utilisateur(Base):
    __tablename__ = "utilisateurs"
    id = Column(Integer, primary_key=True, index=True)
    entreprise_id = Column(
        Integer, ForeignKey("entreprises.id", ondelete="CASCADE"), nullable=False
    )
    nom = Column(String(50), nullable=False)
    prenom = Column(String(50), nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    mot_de_passe_hash = Column(Text, nullable=False)
    role = Column(String(20), default="admin")
    actif = Column(Boolean, default=True)
    date_creation = Column(DateTime(timezone=True), default=datetime.datetime.utcnow)

    entreprise = relationship("Entreprise")


class Contact(Base):
    __tablename__ = "contacts"
    id = Column(Integer, primary_key=True, index=True)
    entreprise_id = Column(
        Integer, ForeignKey("entreprises.id", ondelete="CASCADE"), nullable=False
    )
    prenom = Column(String(100))
    nom = Column(String(100))
    entreprise = Column(String(150))
    siret = Column(String(14))
    tva_intra = Column(String(20))
    type_entite = Column(String(20), default="B2B")
    poste = Column(String(150))
    adresse_livraison = Column(Text)
    adresse_facturation = Column(Text)
    email = Column(String(255), index=True, nullable=False)
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
    entreprise_id = Column(
        Integer, ForeignKey("entreprises.id", ondelete="CASCADE"), nullable=False
    )
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
    entreprise_id = Column(
        Integer, ForeignKey("entreprises.id", ondelete="CASCADE"), nullable=False
    )
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
    entreprise_id = Column(
        Integer, ForeignKey("entreprises.id", ondelete="CASCADE"), nullable=False
    )
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
    entreprise_id = Column(
        Integer, ForeignKey("entreprises.id", ondelete="CASCADE"), nullable=False
    )
    reference = Column(String(100))
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
    entreprise_id = Column(
        Integer, ForeignKey("entreprises.id", ondelete="CASCADE"), nullable=False
    )
    numero_facture = Column(String(50), nullable=False)
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
    categorie = Column(String(50), default="AUTRE")

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
        "https://n8n.mrliw.fr/webhook/envoi_factures",
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


def _facture_platform_validated_or_sent_clause():
    plat = func.lower(func.trim(func.coalesce(Facture.statut_plateforme, "")))
    return plat.in_(("validated", "sent"))


def _facture_signed_ttc_reste(facture: Facture) -> tuple[Decimal, Decimal]:
    is_avoir = (facture.type_facture or "").strip().lower() == "avoir"
    sign = Decimal("-1.00") if is_avoir else Decimal("1.00")
    montant_ttc = (_money_dec(facture.montant_ttc) * sign).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    montant_paye = (_money_dec(facture.montant_paye) * sign).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    if montant_ttc <= 0:
        reste = Decimal("0.00")
    else:
        reste = (montant_ttc - montant_paye).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        if reste < 0:
            reste = Decimal("0.00")
    return montant_ttc, reste


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
    categorie: Optional[str] = "AUTRE"
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


class FactureAvoirBody(BaseModel):
    raison: str
    montant: float


class FactureCategorieUpdate(BaseModel):
    categorie: str


class FacturePlateformeUpdate(BaseModel):
    statut_plateforme: str
    envoyer_mail: bool = False


class RegistreGenerateBody(BaseModel):
    type_document: str
    date_debut: datetime.date
    date_fin: datetime.date


class DashboardKpiSchema(BaseModel):
    year: int
    month: int
    date_debut: datetime.date
    date_fin: datetime.date
    vente_ht: float
    vente_ttc: float
    achat_ht: float
    achat_ttc: float
    impots_ht: float
    impots_ttc: float
    resultat_avant_impots_ht: float
    resultat_avant_impots_ttc: float
    resultat_apres_impots_ht: float
    resultat_apres_impots_ttc: float
    nb_factures: int


class DashboardKpiYearSchema(BaseModel):
    year: int
    date_debut: datetime.date
    date_fin: datetime.date
    vente_ht: float
    vente_ttc: float
    achat_ht: float
    achat_ttc: float
    impots_ht: float
    impots_ttc: float
    resultat_avant_impots_ht: float
    resultat_avant_impots_ttc: float
    resultat_apres_impots_ht: float
    resultat_apres_impots_ttc: float
    nb_factures: int


FACTURE_CATEGORIES = (
    "PRESTATION",
    "ABONNEMENT",
    "MATERIEL",
    "CONSOMMABLES",
    "LOGICIEL",
    "FRAIS_DEPLACEMENT",
    "SOUS_TRAITANCE",
    "ASSURANCE",
    "IMPOTS_TAXES",
    "AUTRE",
)

FACTURE_CATEGORIES_VALIDES = frozenset(FACTURE_CATEGORIES)


class DashboardChartsSchema(BaseModel):
    mois_debut: str
    mois_fin: str
    months: List[str]
    categories: List[str]
    achats_ttc: List[float]
    ventes_ttc: List[float]
    resultat_apres_impots_ht: List[float]
    ventes_par_categorie: Dict[str, List[float]]
    achats_par_categorie: Dict[str, List[float]]


class DashboardPieChartsSchema(BaseModel):
    mois_debut: str
    mois_fin: str
    categories: List[str]
    achats_par_categorie: Dict[str, float]
    ventes_par_categorie: Dict[str, float]
    achats_total: float
    ventes_total: float


class DashboardPendingFluxSchema(BaseModel):
    nb_factures: int
    total_ttc: float
    total_reste_ttc: float


class DashboardPendingPaymentsSchema(BaseModel):
    achats: DashboardPendingFluxSchema
    ventes: DashboardPendingFluxSchema


# --- Initialisation FastAPI ---
app = FastAPI(
    title="Dashboard CRM personnel", description="API pour CRM dashboard.mrliw.fr"
)

# Dernier middleware ajouté = exécuté en premier : session avant auth.
setup_auth_middleware(app, Utilisateur, SessionLocal)
setup_session_middleware(app)


def eid(request: Request) -> int:
    entreprise_id = getattr(request.state, "entreprise_id", None)
    if entreprise_id is None:
        raise HTTPException(status_code=401, detail="Non authentifié.")
    return int(entreprise_id)


class LoginBody(BaseModel):
    email: str
    password: str


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


# --- Authentification (API) ---
@app.post("/api/auth/login")
def auth_login(body: LoginBody, request: Request, db: Session = Depends(get_db)):
    email = (body.email or "").strip().lower()
    user = db.query(Utilisateur).filter(Utilisateur.email == email).first()
    if not user or not user.actif or not verify_password(body.password, user.mot_de_passe_hash):
        raise HTTPException(status_code=401, detail="Email ou mot de passe incorrect.")
    request.session["user_id"] = user.id
    return {"status": "ok", "redirect": "/dashboard"}


@app.post("/api/auth/logout")
def auth_logout(request: Request):
    request.session.clear()
    return {"status": "ok"}


@app.get("/api/auth/me")
def auth_me(request: Request, db: Session = Depends(get_db)):
    user = get_session_user(request, db, Utilisateur)
    if not user:
        raise HTTPException(status_code=401, detail="Non authentifié.")
    entreprise = db.query(Entreprise).filter(Entreprise.id == user.entreprise_id).first()
    return {
        "id": user.id,
        "email": user.email,
        "nom": user.nom,
        "prenom": user.prenom,
        "role": user.role,
        "entreprise_id": user.entreprise_id,
        "entreprise_nom": entreprise.nom_usage if entreprise else None,
        "is_primary_user": is_primary_user(user),
    }


UTILISATEUR_ROLES_VALIDES = frozenset({"admin", "user"})


class UtilisateurSchema(BaseModel):
    id: int
    nom: str
    prenom: str
    email: str
    role: str
    actif: bool
    date_creation: Optional[datetime.datetime] = None

    class Config:
        from_attributes = True


class UtilisateurCreateBody(BaseModel):
    nom: str
    prenom: str
    email: str
    password: str
    role: str = "user"
    actif: bool = True


class UtilisateurUpdateBody(BaseModel):
    nom: str
    prenom: str
    email: str
    password: Optional[str] = ""
    role: str
    actif: bool


def _normalize_utilisateur_role(role: str) -> str:
    r = (role or "user").strip().lower()
    if r not in UTILISATEUR_ROLES_VALIDES:
        raise HTTPException(status_code=400, detail="Rôle invalide (admin ou user).")
    return r


# --- Routes de l'API ---
@app.get("/api/utilisateurs", response_model=List[UtilisateurSchema])
def list_utilisateurs(request: Request, db: Session = Depends(get_db)):
    require_admin(request, db, Utilisateur)
    rows = (
        scoped(db, Utilisateur, eid(request))
        .order_by(Utilisateur.nom, Utilisateur.prenom)
        .all()
    )
    return rows


@app.get("/api/utilisateurs/{user_id}", response_model=UtilisateurSchema)
def get_utilisateur(user_id: int, request: Request, db: Session = Depends(get_db)):
    require_admin(request, db, Utilisateur)
    return get_one(db, Utilisateur, user_id, eid(request), "Utilisateur non trouvé.")


@app.post("/api/utilisateurs", response_model=UtilisateurSchema)
def create_utilisateur_api(
    body: UtilisateurCreateBody, request: Request, db: Session = Depends(get_db)
):
    require_admin(request, db, Utilisateur)
    email = (body.email or "").strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Email requis.")
    if not (body.password or "").strip():
        raise HTTPException(status_code=400, detail="Mot de passe requis.")
    if db.query(Utilisateur).filter(Utilisateur.email == email).first():
        raise HTTPException(status_code=400, detail="Cet email est déjà utilisé.")
    role = _normalize_utilisateur_role(body.role)
    user = Utilisateur(
        entreprise_id=eid(request),
        nom=(body.nom or "").strip(),
        prenom=(body.prenom or "").strip(),
        email=email,
        mot_de_passe_hash=hash_password(body.password),
        role=role,
        actif=bool(body.actif),
    )
    if not user.nom or not user.prenom:
        raise HTTPException(status_code=400, detail="Nom et prénom requis.")
    db.add(user)
    try:
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    db.refresh(user)
    return user


@app.put("/api/utilisateurs/{user_id}", response_model=UtilisateurSchema)
def update_utilisateur_api(
    user_id: int,
    body: UtilisateurUpdateBody,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin(request, db, Utilisateur)
    user = get_one(db, Utilisateur, user_id, eid(request), "Utilisateur non trouvé.")
    email = (body.email or "").strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Email requis.")
    existing = db.query(Utilisateur).filter(Utilisateur.email == email).first()
    if existing and existing.id != user.id:
        raise HTTPException(status_code=400, detail="Cet email est déjà utilisé.")
    nom = (body.nom or "").strip()
    prenom = (body.prenom or "").strip()
    if not nom or not prenom:
        raise HTTPException(status_code=400, detail="Nom et prénom requis.")
    user.nom = nom
    user.prenom = prenom
    user.email = email
    user.role = _normalize_utilisateur_role(body.role)
    user.actif = bool(body.actif)
    if (body.password or "").strip():
        user.mot_de_passe_hash = hash_password(body.password)
    try:
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    db.refresh(user)
    return user


@app.get("/api/data", response_model=List[RequeteSchema])
def get_data(request: Request, db: Session = Depends(get_db)):
    require_primary_user(request, db, Utilisateur)
    return scoped(db, Requete, eid(request)).all()


@app.patch("/api/requete/{requete_id}/statut", response_model=RequeteSchema)
def update_statut(
    requete_id: int,
    statut_update: StatutUpdate,
    request: Request,
    db: Session = Depends(get_db),
):
    require_primary_user(request, db, Utilisateur)
    requete = get_one(db, Requete, requete_id, eid(request), "Requête non trouvée")
    if statut_update.statut not in ["nouveau", "traite"]:
        raise HTTPException(status_code=400, detail="Statut invalide.")

    requete.statut = statut_update.statut
    db.commit()
    db.refresh(requete)
    return requete


@app.get("/api/contacts", response_model=List[ContactSchema])
def get_contacts(request: Request, db: Session = Depends(get_db)):
    return scoped(db, Contact, eid(request)).all()


@app.post("/api/contacts", response_model=ContactSchema)
def create_contact(
    contact: ContactCreate, request: Request, db: Session = Depends(get_db)
):
    db_contact = (
        scoped(db, Contact, eid(request))
        .filter(Contact.email == contact.email)
        .first()
    )
    if db_contact:
        raise HTTPException(status_code=400, detail="Le contact est déjà enregistré.")
    new_contact = Contact(
        entreprise_id=eid(request),
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
    contact_id: int,
    contact_update: ContactCreate,
    request: Request,
    db: Session = Depends(get_db),
):
    contact = get_one(db, Contact, contact_id, eid(request), "Contact non trouvé")

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
def get_actions(request: Request, db: Session = Depends(get_db)):
    actions = scoped(db, Action, eid(request)).all()
    return [ActionSchema.from_orm(a) for a in actions]


@app.patch("/api/actions/{action_id}/statut", response_model=ActionSchema)
def update_action_statut(
    action_id: int,
    statut_update: ActionStatutUpdate,
    request: Request,
    db: Session = Depends(get_db),
):
    action = get_one(db, Action, action_id, eid(request), "Action non trouvée")

    action.statut = statut_update.statut
    db.commit()
    db.refresh(action)
    return action


@app.get("/api/commandes", response_model=List[CommandeSchema])
def get_commandes(request: Request, db: Session = Depends(get_db)):
    return scoped(db, Commande, eid(request)).all()


@app.patch("/api/commandes/{commande_id}/statut", response_model=CommandeSchema)
def update_commande_statut(
    commande_id: int,
    statut_update: StatutCommandeUpdate,
    request: Request,
    db: Session = Depends(get_db),
):
    commande = get_one(
        db, Commande, commande_id, eid(request), "Commande non trouvée"
    )

    commande.statut = statut_update.statut
    db.commit()
    db.refresh(commande)
    return commande


@app.patch("/api/commandes/{commande_id}", response_model=CommandeSchema)
def update_commande(
    commande_id: int,
    payload: CommandeUpdate,
    request: Request,
    db: Session = Depends(get_db),
):
    commande = get_one(
        db, Commande, commande_id, eid(request), "Commande non trouvée"
    )
    get_one(db, Contact, payload.contact_id, eid(request), "Contact introuvable")

    if payload.devis_id:
        get_one(db, Devis, payload.devis_id, eid(request), "Devis introuvable")

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
    request: Request,
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
    get_one(db, Contact, contact_id, eid(request), "Contact introuvable")

    if devis_id:
        get_one(db, Devis, devis_id, eid(request), "Devis introuvable")

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
        entreprise_id=eid(request),
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
def factures_unpaid_stats(request: Request, db: Session = Depends(get_db)):
    tenant_id = eid(request)
    unpaid = (
        scoped(db, Facture, tenant_id)
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
        scoped(db, Facture, tenant_id)
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
    request: Request,
    ref: Optional[str] = None,
    client: Optional[str] = None,
    include_paid: bool = True,
    db: Session = Depends(get_db),
):
    tenant_id = eid(request)
    q = scoped(db, Facture, tenant_id).options(
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
            for row in scoped(db, Contact, tenant_id)
            .with_entities(Contact.id)
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
    request: Request,
    db: Session = Depends(get_db),
):
    raw = (body.statut_plateforme or "").strip().lower()
    if raw not in ("validated", "rejected", "sent"):
        raise HTTPException(
            status_code=400,
            detail="statut_plateforme doit être « validated », « rejected » ou « sent ».",
        )
    require_primary_user(request, db, Utilisateur)
    facture = (
        scoped(db, Facture, eid(request))
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


@app.patch("/api/factures/{facture_id}/categorie", response_model=FactureSchema)
def update_facture_categorie(
    facture_id: int,
    body: FactureCategorieUpdate,
    request: Request,
    db: Session = Depends(get_db),
):
    categorie = (body.categorie or "AUTRE").strip().upper() or "AUTRE"
    if categorie not in FACTURE_CATEGORIES_VALIDES:
        raise HTTPException(status_code=400, detail="Catégorie de facture invalide.")

    tenant_id = eid(request)
    facture = (
        scoped(db, Facture, tenant_id)
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
    if (facture.type_facture or "Facture").strip().lower() == "avoir":
        raise HTTPException(
            status_code=400,
            detail="La catégorie d'un avoir se modifie depuis sa facture associée.",
        )

    facture.categorie = categorie
    scoped(db, Facture, tenant_id).filter(
        Facture.id_facture_associee == facture.id,
        func.lower(func.trim(func.coalesce(Facture.type_facture, ""))) == "avoir",
    ).update({Facture.categorie: categorie}, synchronize_session=False)
    db.commit()
    db.refresh(facture)
    return facture


@app.head("/api/factures")
def head_list_factures():
    """Sondes HEAD (healthcheck Docker, proxy) : pas de corps, évite 405 sur GET /api/factures."""
    return Response(status_code=200)


@app.get("/api/factures/{facture_id}", response_model=FactureSchema)
def get_facture(facture_id: int, request: Request, db: Session = Depends(get_db)):
    facture = (
        scoped(db, Facture, eid(request))
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
    request: Request,
    db: Session = Depends(get_db),
):
    ajout = _money_dec(body.montant)
    if ajout <= 0:
        raise HTTPException(
            status_code=400,
            detail="Le montant du versement doit être strictement positif.",
        )
    facture = (
        scoped(db, Facture, eid(request))
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


@app.post("/api/factures/{facture_id}/avoir", response_model=FactureSchema)
def generate_facture_avoir(
    facture_id: int,
    body: FactureAvoirBody,
    request: Request,
    db: Session = Depends(get_db),
):
    raison = (body.raison or "").strip()
    if not raison:
        raise HTTPException(status_code=400, detail="La raison de l'avoir est requise.")

    montant_avoir = _money_dec(body.montant)
    if montant_avoir < 0:
        raise HTTPException(
            status_code=400,
            detail="Le montant de l'avoir ne peut pas être négatif.",
        )

    tenant_id = eid(request)
    facture = (
        scoped(db, Facture, tenant_id)
        .options(joinedload(Facture.contact))
        .filter(Facture.id == facture_id)
        .first()
    )
    if not facture:
        raise HTTPException(status_code=404, detail="Facture non trouvée")

    if (facture.type_facture or "Facture").strip().lower() != "facture":
        raise HTTPException(
            status_code=400,
            detail="Un avoir ne peut être généré que depuis une facture.",
        )

    flux = (facture.flux or "").strip().lower()
    if flux not in ("vente", "envoyée", "envoyee"):
        raise HTTPException(
            status_code=400,
            detail="Un avoir ne peut être généré que pour une facture en flux vente.",
        )

    statut_plateforme = (facture.statut_plateforme or "").strip().lower()
    if statut_plateforme not in ("validated", "sent"):
        raise HTTPException(
            status_code=400,
            detail="Un avoir ne peut être généré que pour une facture plateforme validée ou envoyée.",
        )

    montant_ttc = _money_dec(facture.montant_ttc)
    if montant_avoir > montant_ttc:
        raise HTTPException(
            status_code=400,
            detail="Le montant de l'avoir ne peut pas dépasser le montant TTC de la facture.",
        )

    contact = facture.contact
    if not contact:
        raise HTTPException(status_code=400, detail="Contact client introuvable.")

    ref_avoir = _next_avoir_reference(db, tenant_id)
    entreprise = (contact.entreprise or "").strip()
    prenom_nom = f"{contact.prenom or ''} {contact.nom or ''}".strip()
    nom_client = f"{prenom_nom} ({entreprise})" if entreprise else prenom_nom
    adresse_client = contact.adresse_facturation or contact.adresse_livraison or ""
    generations = generate_avoir_files(
        ref_avoir=ref_avoir,
        ref_facture=facture.numero_facture,
        nom_client=nom_client,
        adresse_client=adresse_client,
        contact_client=contact.email or "",
        description_avoir=raison,
        montant=float(montant_avoir),
        date_facture=facture.date_emission,
    )

    montant_paye_initial = _money_dec(facture.montant_paye)
    montant_paye_avec_avoir = (montant_paye_initial + montant_avoir).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    ecart_montant = Decimal("0.00")
    if montant_paye_avec_avoir > montant_ttc:
        ecart_montant = (montant_paye_avec_avoir - montant_ttc).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        montant_paye_avec_avoir = montant_ttc

    montant_paye_avoir = (montant_avoir - ecart_montant).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    if montant_paye_avoir < 0:
        montant_paye_avoir = Decimal("0.00")

    now = datetime.datetime.now(datetime.timezone.utc)
    facture.montant_paye = montant_paye_avec_avoir
    facture.statut_paiement = _statut_paiement_from_montants(
        montant_paye_avec_avoir, montant_ttc
    )
    facture.date_paiement = now if facture.statut_paiement == "paye" else None

    avoir = Facture(
        entreprise_id=tenant_id,
        numero_facture=ref_avoir,
        contact_id=contact.id,
        devis_id=facture.devis_id,
        commande_id=facture.commande_id,
        flux="vente",
        montant_ht=montant_avoir,
        montant_tva=Decimal("0.00"),
        montant_ttc=montant_avoir,
        devise=facture.devise or "EUR",
        file_path=generations.get("url_path") or f"/files/avoirs/{ref_avoir}.pdf",
        external_id=None,
        statut_plateforme="draft",
        statut_paiement=_statut_paiement_from_montants(
            montant_paye_avoir, montant_avoir
        ),
        date_emission=now,
        date_echeance=None,
        date_paiement=now if montant_paye_avoir >= montant_avoir and montant_avoir > 0 else None,
        type_facture="Avoir",
        montant_paye=montant_paye_avoir,
        categorie=facture.categorie or "AUTRE",
        id_facture_associee=facture.id,
    )
    db.add(avoir)
    try:
        db.commit()
    except Exception as e:
        db.rollback()
        for key in ("html_path", "pdf_path"):
            path = generations.get(key)
            try:
                if path and os.path.isfile(path):
                    os.remove(path)
            except OSError:
                pass
        raise HTTPException(status_code=400, detail=str(e))
    db.refresh(avoir)
    return avoir


@app.post("/api/factures/manuel")
def create_manual_facture(
    request: Request,
    contact_id: int = Form(...),
    flux: str = Form("vente"),
    categorie: str = Form("AUTRE"),
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
    tenant_id = eid(request)
    get_one(db, Contact, contact_id, tenant_id, "Contact introuvable")

    flux_clean = (flux or "").strip().lower()
    if flux_clean in ("vente", "envoyee", "envoyée"):
        flux_db = "vente"
    elif flux_clean in ("achat", "receptionnee", "réceptionnée"):
        flux_db = "achat"
    else:
        raise HTTPException(
            status_code=400,
            detail="flux doit être « vente » ou « achat ».",
        )

    categorie_db = (categorie or "AUTRE").strip().upper() or "AUTRE"
    if categorie_db not in FACTURE_CATEGORIES_VALIDES:
        categorie_db = "AUTRE"

    d_id: Optional[int] = None
    if devis_id and str(devis_id).strip():
        try:
            d_id = int(devis_id)
        except ValueError:
            d_id = None
        if d_id is not None:
            get_one(db, Devis, d_id, tenant_id, "Devis introuvable")

    c_id: Optional[int] = None
    if commande_id and str(commande_id).strip():
        try:
            c_id = int(commande_id)
        except ValueError:
            c_id = None
        if c_id is not None:
            get_one(db, Commande, c_id, tenant_id, "Commande introuvable")

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
    while scoped(db, Facture, tenant_id).filter(Facture.numero_facture == num).first():
        n += 1
        suf = f"-{n}"
        num = (
            (base_num[: 50 - len(suf)] + suf)
            if len(base_num) + len(suf) > 50
            else base_num + suf
        )

    facture = Facture(
        entreprise_id=tenant_id,
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
        categorie=categorie_db,
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
    devis_id: int,
    payload: UpdateDevisPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    devis = get_one(db, Devis, devis_id, eid(request), "Devis non trouvé")

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
def trigger_action_webhook(payload: WebhookPayload, request: Request):
    try:
        data = json.dumps(
            {"action": payload.texte, "entreprise_id": eid(request)}
        ).encode("utf-8")
        req = urllib.request.Request(
            "https://n8n.mrliw.fr/webhook/dashboard-actions",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req) as response:
            return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _resolve_devis_associe_label(
    value: str, db: Session, entreprise_id: int
) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    if value.isdigit():
        devis = (
            scoped(db, Devis, entreprise_id)
            .filter(Devis.id == int(value))
            .first()
        )
        return devis.nom if devis else value
    devis = scoped(db, Devis, entreprise_id).filter(Devis.nom == value).first()
    return devis.nom if devis else value


def _enrich_facture_webhook_response(
    data,
    db: Session,
    entreprise_id: int,
    fallback_devis_id: Optional[int] = None,
):
    if not isinstance(data, dict):
        return data
    assoc = (data.get("devis_associe") or "").strip()
    if assoc:
        data["devis_associe"] = _resolve_devis_associe_label(assoc, db, entreprise_id)
    elif fallback_devis_id:
        devis = (
            scoped(db, Devis, entreprise_id)
            .filter(Devis.id == fallback_devis_id)
            .first()
        )
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
def trigger_factures_webhook(
    payload: WebhookPayload, request: Request, db: Session = Depends(get_db)
):
    texte = (payload.texte or "").strip()
    if not texte:
        raise HTTPException(status_code=400, detail="Le texte ne peut pas être vide.")
    try:
        result = _post_invoices_dashboard_webhook(
            {"texte": texte, "entreprise_id": eid(request)}
        )
        return _enrich_facture_webhook_response(result, db, eid(request))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class ConfirmFacturePayload(BaseModel):
    prenom: Optional[str] = ""
    nom: Optional[str] = ""
    entreprise: Optional[str] = ""
    email: str
    adresse_facturation: Optional[str] = ""
    description: Optional[str] = ""
    categorie: Optional[str] = "AUTRE"
    devis_associe: Optional[str] = ""
    articles: List[ArticlePayload] = []


def _next_facture_reference(db: Session, entreprise_id: int) -> str:
    now = datetime.datetime.now()
    yy = now.strftime("%y")
    mm = now.strftime("%m")
    prefix = f"mrliw_f{yy}{mm}"
    rows = (
        scoped(db, Facture, entreprise_id)
        .with_entities(Facture.numero_facture)
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


def _next_avoir_reference(db: Session, entreprise_id: int) -> str:
    now = datetime.datetime.now()
    yy = now.strftime("%y")
    mm = now.strftime("%m")
    prefix = f"mrliw_a{yy}{mm}"
    rows = (
        scoped(db, Facture, entreprise_id)
        .with_entities(Facture.numero_facture)
        .filter(Facture.numero_facture.like(f"{prefix}%"))
        .all()
    )
    max_xx = -1
    for (num,) in rows:
        suffix = (num or "")[len(prefix) :]
        if num and num.startswith(prefix) and len(suffix) == 2:
            try:
                max_xx = max(max_xx, int(suffix))
            except ValueError:
                pass
    return f"{prefix}{max_xx + 1:02d}"


@app.post("/api/factures/confirm")
def confirm_facture_creation(
    payload: ConfirmFacturePayload, request: Request, db: Session = Depends(get_db)
):
    email = (payload.email or "").strip()
    if not email:
        raise HTTPException(status_code=400, detail="Email client requis.")
    if not payload.articles:
        raise HTTPException(status_code=400, detail="Au moins un article est requis.")

    categorie = (payload.categorie or "AUTRE").strip().upper() or "AUTRE"
    if categorie not in FACTURE_CATEGORIES_VALIDES:
        raise HTTPException(status_code=400, detail="Catégorie de facture invalide.")

    total_ht = Decimal("0")
    for a in payload.articles:
        pu = Decimal(str(a.prix_unitaire))
        q = Decimal(str(a.quantite))
        r = Decimal(str(a.remise or 0))
        total_ht += pu * q * (Decimal("1") - r / Decimal("100"))
    total_ht = total_ht.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    montant_tva = Decimal("0")
    montant_ttc = total_ht

    tenant_id = eid(request)
    contact = scoped(db, Contact, tenant_id).filter(Contact.email == email).first()
    if not contact:
        contact = Contact(entreprise_id=tenant_id, email=email)
        db.add(contact)
    contact.prenom = payload.prenom or ""
    contact.nom = payload.nom or ""
    contact.entreprise = payload.entreprise or ""
    if payload.adresse_facturation:
        contact.adresse_facturation = payload.adresse_facturation
    db.commit()
    db.refresh(contact)

    ref_facture = _next_facture_reference(db, tenant_id)
    entreprise = (payload.entreprise or "").strip()
    prenom_nom = f"{payload.prenom or ''} {payload.nom or ''}".strip()
    nom_client = (
        f"{prenom_nom} ({entreprise})" if entreprise else prenom_nom
    )

    ref_devis = (payload.devis_associe or "").strip()
    devis_id = None
    if ref_devis:
        devis = scoped(db, Devis, tenant_id).filter(Devis.nom == ref_devis).first()
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
        entreprise_id=tenant_id,
        numero_facture=ref_facture,
        contact_id=contact.id,
        devis_id=devis_id,
        commande_id=None,
        flux="vente",
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
        categorie=categorie,
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
    devis_id: int,
    payload: WebhookPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    devis = get_one(db, Devis, devis_id, eid(request), "Devis non trouvé")
    if devis.statut != "Signé":
        raise HTTPException(
            status_code=400,
            detail="Seul un devis signé peut générer une facture.",
        )
    texte = (payload.texte or "").strip()
    if not texte:
        raise HTTPException(status_code=400, detail="Le texte ne peut pas être vide.")
    try:
        result = _post_invoices_dashboard_webhook(
            {
                "texte": texte,
                "devis_id": devis_id,
                "entreprise_id": eid(request),
            }
        )
        return _enrich_facture_webhook_response(
            result, db, eid(request), fallback_devis_id=devis_id
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- Frontend (Static files & Templates) ---
app.mount("/css", StaticFiles(directory="css"), name="css")
app.mount("/js", StaticFiles(directory="js"), name="js")
app.mount("/img", StaticFiles(directory="img"), name="img")
app.mount("/files", StaticFiles(directory="files"), name="files")
app.mount(
    "/app/files", StaticFiles(directory="files"), name="app_files"
)  # Rétro-compatibilité pour les tests
templates = Jinja2Templates(directory="templates")


@app.get("/login", response_class=HTMLResponse)
def page_login(request: Request, db: Session = Depends(get_db)):
    if get_session_user(request, db, Utilisateur):
        return RedirectResponse(url="/dashboard", status_code=303)
    return templates.TemplateResponse(request=request, name="login.html", context={})


@app.get("/")
def root():
    return RedirectResponse(url="/dashboard", status_code=303)


@app.get("/requetes", response_class=HTMLResponse)
def page_requetes(request: Request, db: Session = Depends(get_db)):
    user = get_session_user(request, db, Utilisateur)
    if not is_primary_user(user):
        return RedirectResponse(url="/dashboard", status_code=303)
    return templates.TemplateResponse(request=request, name="index.html", context={})


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


@app.get("/dashboard", response_class=HTMLResponse)
def page_dashboard(request: Request):
    return templates.TemplateResponse(request=request, name="dashboard.html", context={})


def _aggregate_pending_payments(factures: list, flux: str) -> DashboardPendingFluxSchema:
    flux_key = flux.strip().lower()
    total_ttc = Decimal("0.00")
    total_reste = Decimal("0.00")
    nb = 0
    for facture in factures:
        if (facture.flux or "").strip().lower() != flux_key:
            continue
        ttc, reste = _facture_signed_ttc_reste(facture)
        nb += 1
        total_ttc += ttc
        total_reste += reste
    return DashboardPendingFluxSchema(
        nb_factures=nb,
        total_ttc=float(total_ttc.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
        total_reste_ttc=float(
            total_reste.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        ),
    )


@app.get("/api/dashboard/pending-payments", response_model=DashboardPendingPaymentsSchema)
def dashboard_pending_payments(
    request: Request, db: Session = Depends(get_db)
):
    factures = (
        scoped(db, Facture, eid(request))
        .filter(
            _facture_reste_montant_clause(),
            _facture_platform_validated_or_sent_clause(),
        )
        .all()
    )
    return DashboardPendingPaymentsSchema(
        achats=_aggregate_pending_payments(factures, "achat"),
        ventes=_aggregate_pending_payments(factures, "vente"),
    )


def _month_period(year: int, month: int) -> tuple[datetime.date, datetime.date]:
    last_day = calendar.monthrange(year, month)[1]
    return datetime.date(year, month, 1), datetime.date(year, month, last_day)


def _year_period(year: int) -> tuple[datetime.date, datetime.date]:
    return datetime.date(year, 1, 1), datetime.date(year, 12, 31)


def _validate_dashboard_year(year: int) -> None:
    if year < 2000 or year > 2100:
        raise HTTPException(status_code=400, detail="Année invalide.")


def _compute_dashboard_kpi(
    db: Session,
    date_debut: datetime.date,
    date_fin: datetime.date,
    entreprise_id: int,
) -> dict:
    factures = _query_paid_factures(db, date_debut, date_fin, entreprise_id)
    _, totals = _build_registre_data(factures)

    def _f(key: str) -> float:
        return float(totals.get(key, Decimal("0.00")))

    return {
        "date_debut": date_debut,
        "date_fin": date_fin,
        "vente_ht": _f("vente_ht"),
        "vente_ttc": _f("vente_ttc"),
        "achat_ht": _f("achat_ht"),
        "achat_ttc": _f("achat_ttc"),
        "impots_ht": _f("impots_ht"),
        "impots_ttc": _f("impots_ttc"),
        "resultat_avant_impots_ht": _f("resultat_avant_impots_ht"),
        "resultat_avant_impots_ttc": _f("resultat_avant_impots_ttc"),
        "resultat_apres_impots_ht": _f("resultat_ht"),
        "resultat_apres_impots_ttc": _f("resultat_ttc"),
        "nb_factures": len(factures),
    }


def _query_paid_factures(
    db: Session,
    date_debut: datetime.date,
    date_fin: datetime.date,
    entreprise_id: int,
    flux: Optional[str] = None,
) -> list:
    date_debut_dt = datetime.datetime.combine(date_debut, datetime.time.min)
    date_fin_dt = datetime.datetime.combine(date_fin, datetime.time.max)
    statut_expr = func.lower(func.trim(func.coalesce(Facture.statut_paiement, "")))
    flux_expr = func.lower(func.trim(cast(Facture.flux, String)))

    query = (
        scoped(db, Facture, entreprise_id)
        .options(joinedload(Facture.contact))
        .filter(
            statut_expr == "paye",
            Facture.date_paiement.isnot(None),
            Facture.date_paiement >= date_debut_dt,
            Facture.date_paiement <= date_fin_dt,
        )
    )
    if flux:
        query = query.filter(flux_expr == flux.strip().lower())
    return query.order_by(Facture.date_paiement.asc(), Facture.id.asc()).all()


def _build_registre_data(factures: list) -> tuple[list, dict]:
    totals = {
        "vente_ht": Decimal("0.00"),
        "vente_ttc": Decimal("0.00"),
        "achat_ht": Decimal("0.00"),
        "achat_ttc": Decimal("0.00"),
        "impots_ht": Decimal("0.00"),
        "impots_ttc": Decimal("0.00"),
    }
    items = []
    for facture in factures:
        flux = (facture.flux or "").strip().lower()
        if flux not in ("vente", "achat"):
            continue

        is_avoir = (facture.type_facture or "").strip().lower() == "avoir"
        sign = Decimal("-1.00") if is_avoir else Decimal("1.00")
        montant_ht = (_money_dec(facture.montant_ht) * sign).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        montant_tva = (_money_dec(facture.montant_tva) * sign).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        montant_ttc = (_money_dec(facture.montant_ttc) * sign).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

        if flux == "vente":
            totals["vente_ht"] += montant_ht
            totals["vente_ttc"] += montant_ttc
        else:
            totals["achat_ht"] += montant_ht
            totals["achat_ttc"] += montant_ttc
            if (facture.categorie or "").strip().upper() == "IMPOTS_TAXES":
                montant_impot = montant_ht
                totals["impots_ht"] += montant_impot
                totals["impots_ttc"] += montant_impot

        contact = facture.contact
        prenom_nom = (
            f"{(contact.prenom or '').strip()} {(contact.nom or '').strip()}".strip()
            if contact
            else ""
        )
        entreprise = (contact.entreprise or "").strip() if contact else ""
        if entreprise and prenom_nom:
            entite_nom = f"{prenom_nom} ({entreprise})"
        else:
            entite_nom = prenom_nom or entreprise or (contact.email if contact else "")

        items.append(
            {
                "date_paiement": facture.date_paiement,
                "flux": flux,
                "reference": facture.numero_facture,
                "entite_nom": entite_nom,
                "categorie": facture.categorie or "AUTRE",
                "montant_ht": montant_ht,
                "montant_tva": montant_tva,
                "montant_ttc": montant_ttc,
            }
        )

    totals["resultat_ht"] = totals["vente_ht"] - totals["achat_ht"]
    totals["resultat_ttc"] = totals["vente_ttc"] - totals["achat_ttc"]
    totals["resultat_avant_impots_ht"] = totals["resultat_ht"] + totals["impots_ht"]
    totals["resultat_avant_impots_ttc"] = (
        totals["resultat_ttc"] + totals["impots_ttc"]
    )
    return items, totals


@app.get("/api/dashboard/kpi", response_model=DashboardKpiSchema)
def dashboard_kpi(
    request: Request,
    year: Optional[int] = None,
    month: Optional[int] = None,
    db: Session = Depends(get_db),
):
    today = datetime.date.today()
    year = year or today.year
    month = month or today.month
    if month < 1 or month > 12:
        raise HTTPException(status_code=400, detail="Le mois doit être entre 1 et 12.")
    _validate_dashboard_year(year)

    date_debut, date_fin = _month_period(year, month)
    data = _compute_dashboard_kpi(db, date_debut, date_fin, eid(request))
    return DashboardKpiSchema(year=year, month=month, **data)


@app.get("/api/dashboard/kpi-year", response_model=DashboardKpiYearSchema)
def dashboard_kpi_year(
    request: Request,
    year: Optional[int] = None,
    db: Session = Depends(get_db),
):
    today = datetime.date.today()
    year = year or today.year
    _validate_dashboard_year(year)

    date_debut, date_fin = _year_period(year)
    data = _compute_dashboard_kpi(db, date_debut, date_fin, eid(request))
    return DashboardKpiYearSchema(year=year, **data)


def _parse_year_month(value: str) -> tuple[int, int]:
    raw = (value or "").strip()
    parts = raw.split("-")
    if len(parts) != 2:
        raise HTTPException(
            status_code=400, detail="Format de mois invalide (attendu AAAA-MM)."
        )
    try:
        year = int(parts[0])
        month = int(parts[1])
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail="Format de mois invalide (attendu AAAA-MM)."
        ) from exc
    if month < 1 or month > 12:
        raise HTTPException(status_code=400, detail="Le mois doit être entre 1 et 12.")
    _validate_dashboard_year(year)
    return year, month


def _format_year_month(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def _default_chart_period() -> tuple[str, str]:
    today = datetime.date.today()
    end_year, end_month = today.year, today.month
    start_year, start_month = end_year, end_month - 11
    while start_month <= 0:
        start_month += 12
        start_year -= 1
    return _format_year_month(start_year, start_month), _format_year_month(
        end_year, end_month
    )


def _month_keys_between(mois_debut: str, mois_fin: str) -> list[str]:
    y1, m1 = _parse_year_month(mois_debut)
    y2, m2 = _parse_year_month(mois_fin)
    if (y1, m1) > (y2, m2):
        raise HTTPException(
            status_code=400,
            detail="Le mois de début doit être antérieur ou égal au mois de fin.",
        )

    keys: list[str] = []
    year, month = y1, m1
    while (year, month) <= (y2, m2):
        keys.append(_format_year_month(year, month))
        month += 1
        if month > 12:
            month = 1
            year += 1
    if len(keys) > 36:
        raise HTTPException(
            status_code=400, detail="La période ne peut pas dépasser 36 mois."
        )
    return keys


def _normalize_facture_categorie(categorie: Optional[str]) -> str:
    key = (categorie or "AUTRE").strip().upper() or "AUTRE"
    return key if key in FACTURE_CATEGORIES else "AUTRE"


def _facture_payment_month_key(date_paiement) -> Optional[str]:
    if not date_paiement:
        return None
    if isinstance(date_paiement, datetime.datetime):
        date_paiement = date_paiement.date()
    return _format_year_month(date_paiement.year, date_paiement.month)


def _build_monthly_charts_data(
    db: Session, mois_debut: str, mois_fin: str, entreprise_id: int
) -> DashboardChartsSchema:
    month_keys = _month_keys_between(mois_debut, mois_fin)
    y1, m1 = _parse_year_month(mois_debut)
    y2, m2 = _parse_year_month(mois_fin)
    date_debut, _ = _month_period(y1, m1)
    _, date_fin = _month_period(y2, m2)

    n = len(month_keys)
    key_index = {key: idx for idx, key in enumerate(month_keys)}
    ventes_ttc = [Decimal("0.00")] * n
    achats_ttc = [Decimal("0.00")] * n
    ventes_ht = [Decimal("0.00")] * n
    achats_ht = [Decimal("0.00")] * n
    ventes_par_categorie = {
        cat: [Decimal("0.00")] * n for cat in FACTURE_CATEGORIES
    }
    achats_par_categorie = {cat: [Decimal("0.00")] * n for cat in FACTURE_CATEGORIES}

    for facture in _query_paid_factures(db, date_debut, date_fin, entreprise_id):
        flux = (facture.flux or "").strip().lower()
        if flux not in ("vente", "achat"):
            continue
        month_key = _facture_payment_month_key(facture.date_paiement)
        if not month_key or month_key not in key_index:
            continue

        idx = key_index[month_key]
        is_avoir = (facture.type_facture or "").strip().lower() == "avoir"
        sign = Decimal("-1.00") if is_avoir else Decimal("1.00")
        montant_ht = (_money_dec(facture.montant_ht) * sign).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        montant_ttc = (_money_dec(facture.montant_ttc) * sign).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        categorie = _normalize_facture_categorie(facture.categorie)

        if flux == "vente":
            ventes_ttc[idx] += montant_ttc
            ventes_ht[idx] += montant_ht
            ventes_par_categorie[categorie][idx] += montant_ttc
        else:
            achats_ttc[idx] += montant_ttc
            achats_ht[idx] += montant_ht
            achats_par_categorie[categorie][idx] += montant_ttc

    def _series(values: list[Decimal]) -> list[float]:
        return [float(v.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)) for v in values]

    resultat_apres_impots_ht = [
        (ventes_ht[i] - achats_ht[i]).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        for i in range(n)
    ]

    return DashboardChartsSchema(
        mois_debut=mois_debut,
        mois_fin=mois_fin,
        months=month_keys,
        categories=list(FACTURE_CATEGORIES),
        achats_ttc=_series(achats_ttc),
        ventes_ttc=_series(ventes_ttc),
        resultat_apres_impots_ht=_series(resultat_apres_impots_ht),
        ventes_par_categorie={
            cat: _series(ventes_par_categorie[cat]) for cat in FACTURE_CATEGORIES
        },
        achats_par_categorie={
            cat: _series(achats_par_categorie[cat]) for cat in FACTURE_CATEGORIES
        },
    )


@app.get("/api/dashboard/charts", response_model=DashboardChartsSchema)
def dashboard_charts(
    request: Request,
    mois_debut: Optional[str] = None,
    mois_fin: Optional[str] = None,
    db: Session = Depends(get_db),
):
    default_debut, default_fin = _default_chart_period()
    mois_debut = (mois_debut or default_debut).strip()
    mois_fin = (mois_fin or default_fin).strip()
    return _build_monthly_charts_data(db, mois_debut, mois_fin, eid(request))


def _default_pie_period() -> tuple[str, str]:
    today = datetime.date.today()
    return _format_year_month(today.year, 1), _format_year_month(
        today.year, today.month
    )


def _build_pie_charts_data(
    db: Session, mois_debut: str, mois_fin: str, entreprise_id: int
) -> DashboardPieChartsSchema:
    monthly = _build_monthly_charts_data(db, mois_debut, mois_fin, entreprise_id)
    achats_par_categorie: dict[str, float] = {}
    ventes_par_categorie: dict[str, float] = {}
    for cat in FACTURE_CATEGORIES:
        achats_par_categorie[cat] = round(
            sum(monthly.achats_par_categorie.get(cat, [])), 2
        )
        ventes_par_categorie[cat] = round(
            sum(monthly.ventes_par_categorie.get(cat, [])), 2
        )
    return DashboardPieChartsSchema(
        mois_debut=mois_debut,
        mois_fin=mois_fin,
        categories=list(FACTURE_CATEGORIES),
        achats_par_categorie=achats_par_categorie,
        ventes_par_categorie=ventes_par_categorie,
        achats_total=round(sum(achats_par_categorie.values()), 2),
        ventes_total=round(sum(ventes_par_categorie.values()), 2),
    )


@app.get("/api/dashboard/pie-charts", response_model=DashboardPieChartsSchema)
def dashboard_pie_charts(
    request: Request,
    mois_debut: Optional[str] = None,
    mois_fin: Optional[str] = None,
    db: Session = Depends(get_db),
):
    default_debut, default_fin = _default_pie_period()
    mois_debut = (mois_debut or default_debut).strip()
    mois_fin = (mois_fin or default_fin).strip()
    return _build_pie_charts_data(db, mois_debut, mois_fin, eid(request))


@app.post("/api/registres")
def generate_registre(
    payload: RegistreGenerateBody, request: Request, db: Session = Depends(get_db)
):
    document_configs = {
        "registre_achats": {
            "label": "Registre des achats",
            "flux": "achat",
            "file_prefix": "reg_achat",
        },
        "registre_ventes": {
            "label": "Registre des ventes",
            "flux": "vente",
            "file_prefix": "reg_ventes",
        },
        "livre_comptes": {
            "label": "Livre des comptes",
            "flux": None,
            "file_prefix": "liv_comptes",
        },
    }
    type_document = (payload.type_document or "").strip().lower()
    config = document_configs.get(type_document)
    if not config:
        raise HTTPException(status_code=400, detail="Type de document invalide.")
    if payload.date_debut > payload.date_fin:
        raise HTTPException(
            status_code=400,
            detail="La date de début doit être antérieure ou égale à la date de fin.",
        )

    factures = _query_paid_factures(
        db,
        payload.date_debut,
        payload.date_fin,
        eid(request),
        flux=config["flux"],
    )
    items, totals = _build_registre_data(factures)

    generations = generate_registre_files(
        document_type=type_document,
        document_label=config["label"],
        date_debut=payload.date_debut,
        date_fin=payload.date_fin,
        items=items,
        totals=totals,
        file_prefix=config["file_prefix"],
    )
    if not os.path.exists(generations["pdf_path"]):
        raise HTTPException(
            status_code=500,
            detail="Le document HTML a été généré, mais la conversion PDF a échoué.",
        )

    return {"status": "success", "file_path": generations["url_path"]}


@app.get("/api/devis", response_model=List[DevisSchema])
def get_devis_list(request: Request, db: Session = Depends(get_db)):
    return scoped(db, Devis, eid(request)).all()


@app.patch("/api/devis/{devis_id}/statut", response_model=DevisSchema)
def update_devis_statut(
    devis_id: int,
    statut_update: DevisStatutUpdate,
    request: Request,
    db: Session = Depends(get_db),
):
    devis_item = get_one(db, Devis, devis_id, eid(request), "Devis non trouvé")

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
    devis_id: int,
    request: Request,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    devis_item = get_one(db, Devis, devis_id, eid(request), "Devis non trouvé")
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
    request: Request,
    nom: str = Form(""),
    client: int = Form(...),
    type: str = Form("émis"),
    description: str = Form(""),
    montant_ht: float = Form(0.0),
    montant_tva: float = Form(0.0),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    get_one(db, Contact, client, eid(request), "Client introuvable")

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
        entreprise_id=eid(request),
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
def trigger_devis_webhook(payload: WebhookPayload, request: Request):
    try:
        data = json.dumps(
            {"devis_request": payload.texte, "entreprise_id": eid(request)}
        ).encode("utf-8")
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
def confirm_devis_creation(
    payload: ConfirmDevisPayload, request: Request, db: Session = Depends(get_db)
):
    try:
        tenant_id = eid(request)
        contact = (
            scoped(db, Contact, tenant_id)
            .filter(Contact.email == payload.email)
            .first()
        )
        if not contact:
            contact = Contact(entreprise_id=tenant_id, email=payload.email)
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
            devis = get_one(
                db,
                Devis,
                payload.id,
                tenant_id,
                "Devis non trouvé pour la mise à jour.",
            )
            ref_devis = devis.nom
        else:
            now = datetime.datetime.now()
            month = now.strftime("%m")
            year = now.strftime("%y")
            count = (
                scoped(db, Devis, tenant_id)
                .filter(Devis.nom.like(f"mrliw_d{month}{year}%"))
                .count()
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
                entreprise_id=tenant_id,
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
        # Webhook final vers N8N (envoi email — utilisateur principal uniquement)
        user = get_session_user(request, db, Utilisateur)
        if payload.envoi == 1 and is_primary_user(user):
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


@app.get("/utilisateurs", response_class=HTMLResponse)
def page_utilisateurs(request: Request, db: Session = Depends(get_db)):
    user = get_session_user(request, db, Utilisateur)
    if not user or (user.role or "").strip().lower() != "admin":
        return RedirectResponse(url="/dashboard", status_code=303)
    return templates.TemplateResponse(
        request=request, name="utilisateurs.html", context={}
    )
