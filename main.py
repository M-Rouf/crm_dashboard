from fastapi import FastAPI, Depends, HTTPException, Request, UploadFile, File
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, DateTime, Text, Numeric
from sqlalchemy.orm import sessionmaker, Session, declarative_base, relationship
from pydantic import BaseModel
from typing import List, Optional
import datetime
import os
import urllib.request
import json
import shutil
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from scripts.generate_devis import generate_devis_files

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
    poste = Column(String(150))
    adresse_livraison = Column(Text)
    adresse_facturation = Column(Text)
    email = Column(String(255), unique=True, index=True, nullable=False)
    telephone = Column(String(30))
    date_creation = Column(DateTime(timezone=True), default=datetime.datetime.utcnow)
    
    requetes = relationship("Requete", back_populates="contact")
    devis = relationship("Devis", back_populates="contact")

class Requete(Base):
    __tablename__ = "requetes"
    id = Column(Integer, primary_key=True, index=True)
    contact_id = Column(Integer, ForeignKey("contacts.id", ondelete="CASCADE"), nullable=False)
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
    client = Column(String(255))
    detail = Column(Text)
    priorite = Column(String(50), default="normale")
    statut = Column(String(50), default="nouveau")
    date = Column(DateTime(timezone=True), default=datetime.datetime.utcnow)

class Devis(Base):
    __tablename__ = "devis"
    id = Column(Integer, primary_key=True, index=True)
    nom = Column(String(255), nullable=False)
    client = Column(Integer, ForeignKey("contacts.id", ondelete="CASCADE"), nullable=False)
    description = Column(Text)
    montant_ht = Column(Numeric(10, 2))
    file_path = Column(Text)
    statut = Column(String(50), default="En attente")
    type = Column(String(50), default="émis")
    date_emission = Column(DateTime(timezone=True), default=datetime.datetime.utcnow)

    contact = relationship("Contact", back_populates="devis")

# --- Schémas Pydantic ---
class ContactSchema(BaseModel):
    id: int
    prenom: Optional[str] = None
    nom: Optional[str] = None
    entreprise: Optional[str] = None
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
    client: Optional[str] = None
    detail: Optional[str] = None
    priorite: Optional[str] = "normale"
    statut: Optional[str] = "nouveau"
    date: Optional[datetime.datetime] = None

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

# --- Initialisation FastAPI ---
app = FastAPI(title="Dashboard CRM personnel", description="API pour CRM dashboard.mrliw.fr")

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
def update_statut(requete_id: int, statut_update: StatutUpdate, db: Session = Depends(get_db)):
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
        poste=contact.poste,
        email=contact.email,
        telephone=contact.telephone,
        adresse_livraison=contact.adresse_livraison,
        adresse_facturation=contact.adresse_facturation
    )
    db.add(new_contact)
    db.commit()
    db.refresh(new_contact)
    return new_contact

@app.put("/api/contacts/{contact_id}", response_model=ContactSchema)
def update_contact(contact_id: int, contact_update: ContactCreate, db: Session = Depends(get_db)):
    contact = db.query(Contact).filter(Contact.id == contact_id).first()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact non trouvé")
    
    contact.prenom = contact_update.prenom
    contact.nom = contact_update.nom
    contact.entreprise = contact_update.entreprise
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
    return db.query(Action).all()

@app.patch("/api/actions/{action_id}/statut", response_model=ActionSchema)
def update_action_statut(action_id: int, statut_update: ActionStatutUpdate, db: Session = Depends(get_db)):
    action = db.query(Action).filter(Action.id == action_id).first()
    if not action:
        raise HTTPException(status_code=404, detail="Action non trouvée")
    
    action.statut = statut_update.statut
    db.commit()
    db.refresh(action)
    return action

class WebhookPayload(BaseModel):
    texte: str

class ArticlePayload(BaseModel):
    designation: str
    quantite: int
    prix_unitaire: float

class ConfirmDevisPayload(BaseModel):
    id: Optional[int] = None
    nom: Optional[str] = ""
    prenom: Optional[str] = ""
    entreprise: Optional[str] = ""
    adresse_facturation: Optional[str] = ""
    adresse_livraison: Optional[str] = ""
    email: str
    articles: List[ArticlePayload] = []
    total_estime: float = 0.0
    note: Optional[str] = ""
    texte: str
    designation: Optional[str] = ""

class UpdateDevisPayload(BaseModel):
    texte: str

@app.post("/api/devis/{devis_id}/update_webhook")
def trigger_update_devis_webhook(devis_id: int, payload: UpdateDevisPayload, db: Session = Depends(get_db)):
    devis = db.query(Devis).filter(Devis.id == devis_id).first()
    if not devis:
        raise HTTPException(status_code=404, detail="Devis non trouvé")
    
    try:
        webhook_data = {
            "ref_devis": devis.nom,
            "description_de_devis": devis.description,
            "texte_modification": payload.texte
        }
        data = json.dumps(webhook_data).encode('utf-8')
        req = urllib.request.Request(
            "https://n8n.mrliw.fr/webhook/update_devis",
            data=data,
            headers={'Content-Type': 'application/json'}
        )
        response = urllib.request.urlopen(req, timeout=120)
        response_body = response.read().decode('utf-8')
        return json.loads(response_body) if response_body else {"status": "envoyé"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/actions/webhook")
def trigger_action_webhook(payload: WebhookPayload):
    try:
        data = json.dumps({"action": payload.texte}).encode('utf-8')
        req = urllib.request.Request(
            "https://n8n.mrliw.fr/webhook/dashboard-actions",
            data=data,
            headers={'Content-Type': 'application/json'}
        )
        with urllib.request.urlopen(req) as response:
            return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- Frontend (Static files & Templates) ---
app.mount("/css", StaticFiles(directory="css"), name="css")
app.mount("/img", StaticFiles(directory="img"), name="img")
app.mount("/files", StaticFiles(directory="files"), name="files")
app.mount("/app/files", StaticFiles(directory="files"), name="app_files") # Rétro-compatibilité pour les tests
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse(
    request=request, 
    name="index.html", 
    context={} # Tu peux ajouter tes variables ici si besoin
    )

@app.get("/contacts", response_class=HTMLResponse)
def page_contacts(request: Request):
    return templates.TemplateResponse(
        request=request, 
        name="contacts.html", 
        context={}
    )

@app.get("/actions", response_class=HTMLResponse)
def page_actions(request: Request):
    return templates.TemplateResponse(
        request=request, 
        name="actions.html", 
        context={}
    )

@app.get("/api/devis", response_model=List[DevisSchema])
def get_devis_list(db: Session = Depends(get_db)):
    devis_all = db.query(Devis).all()
    return devis_all

@app.patch("/api/devis/{devis_id}/statut", response_model=DevisSchema)
def update_devis_statut(devis_id: int, statut_update: DevisStatutUpdate, db: Session = Depends(get_db)):
    devis_item = db.query(Devis).filter(Devis.id == devis_id).first()
    if not devis_item:
        raise HTTPException(status_code=404, detail="Devis non trouvé")
    
    if devis_item.statut == "Signé":
        raise HTTPException(status_code=400, detail="Un devis signé ne peut pas être modifié")
    
    devis_item.statut = statut_update.statut
    db.commit()
    db.refresh(devis_item)
    return devis_item

@app.post("/api/devis/{devis_id}/upload_signed")
def upload_signed_devis(devis_id: int, file: UploadFile = File(...), db: Session = Depends(get_db)):
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

@app.post("/api/devis/webhook")
def trigger_devis_webhook(payload: WebhookPayload):
    try:
        data = json.dumps({"devis_request": payload.texte}).encode('utf-8')
        req = urllib.request.Request(
            "https://n8n.mrliw.fr/webhook/devis_dashboard_request",
            data=data,
            headers={'Content-Type': 'application/json'}
        )
        with urllib.request.urlopen(req) as response:
            res_body = response.read().decode('utf-8')
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
        if payload.adresse_facturation:
            contact.adresse_facturation = payload.adresse_facturation
        if payload.adresse_livraison:
            contact.adresse_livraison = payload.adresse_livraison
        
        db.commit()
        db.refresh(contact)

        if payload.id:
            devis = db.query(Devis).filter(Devis.id == payload.id).first()
            if not devis:
                raise HTTPException(status_code=404, detail="Devis non trouvé pour la mise à jour.")
            ref_devis = devis.nom
        else:
            now = datetime.datetime.now()
            month = now.strftime("%m")
            year = now.strftime("%y")
            count = db.query(Devis).filter(Devis.nom.like(f"mrliw_d{month}{year}%")).count()
            ref_devis = f"mrliw_d{month}{year}{count:02d}"

        nom_client = f"{payload.prenom} {payload.nom} ({payload.entreprise})" if payload.entreprise else f"{payload.prenom} {payload.nom}"
        contact_client = payload.email
        if contact.telephone:
            contact_client = f"{contact.telephone} | {payload.email}"

        generations = generate_devis_files(
            ref_devis=ref_devis,
            nom_client=nom_client,
            adresse_client=payload.adresse_livraison or "",
            contact_client=contact_client,
            articles=payload.articles,
            total_ht=payload.total_estime
        )

        desc_lines = ["Articles:"]
        for a in payload.articles:
            desc_lines.append(f"- {a.quantite}x {a.designation} ({a.prix_unitaire}€)")
        if payload.note:
            desc_lines.append(f"\nNote: {payload.note}")

        file_path = generations.get("url_path", generations["pdf_path"])
        if payload.id:
            devis.description = "\n".join(desc_lines)
            devis.montant_ht = payload.total_estime
            devis.file_path = file_path
        else:
            devis = Devis(
                nom=ref_devis,
                client=contact.id,
                description="\n".join(desc_lines),
                montant_ht=payload.total_estime,
                statut="En attente",
                file_path=file_path
            )
            db.add(devis)
        
        db.commit()
        db.refresh(devis)
        # Webhook final vers N8N
        try:
            webhook_payload = {
                "prenom": payload.prenom,
                "email": payload.email,
                "nom_devis": ref_devis,
                "designation_devis": payload.designation,
                "file_path": devis.file_path
            }
            data = json.dumps(webhook_payload).encode('utf-8')
            req = urllib.request.Request(
                "https://n8n.mrliw.fr/webhook/envoi_devis",
                data=data,
                headers={'Content-Type': 'application/json'}
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
    return templates.TemplateResponse(
        request=request, 
        name="devis.html", 
        context={}
    )
