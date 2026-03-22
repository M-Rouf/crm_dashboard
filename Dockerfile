FROM python:3.11-slim

WORKDIR /app

# Empêcher Python d'écrire des fichiers .pyc locaux
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Copie et installation des dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie du code source
COPY . .

# Port exposé (FastAPI tourne généralement sur 8000)
EXPOSE 8000

# Commande pour démarrer Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
