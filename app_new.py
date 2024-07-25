from fastapi import FastAPI, HTTPException, Query, Depends
from pydantic import BaseModel
from typing import List, Optional
import json
import pandas as pd
import numpy as np
import uvicorn
import math
# Charger les données JSON depuis le fichier
try:
    with open('Boutiques.json', 'r') as f:
        data = json.load(f)
except FileNotFoundError:
    print("Le fichier boutiques.json est introuvable.")
    data = []
except json.JSONDecodeError:
    print("Erreur de déchiffrement JSON.")
    data = []
except Exception as e:
    print(f"Une erreur est survenue : {str(e)}")
    data = []

# Modèle Pydantic pour représenter les données JSON
class DataItem(BaseModel):
    msisdn: str
    refill: str
    password: str
    entity_type_name: str
    entity_name: List[str]

# Instanciation de l'application FastAPI
app = FastAPI()

# Fonction d'authentification
def authenticate_user(msisdn: str, password: str):
    for entry in data:
        if entry['msisdn'] == msisdn:
            if entry['password'] == password:
                return entry
            else:
                raise HTTPException(status_code=401, detail="Mot de passe incorrect")
    raise HTTPException(status_code=404, detail="MSISDN non trouvé")

# Route pour filtrer les données avec authentification
@app.get("/msisdn-user/")
def filter_data(
    msisdn: str,
    password: str,
    entity_type_name: Optional[str] = None
):
    user = authenticate_user(msisdn, password)
    filtered_results = []

    for entry in data:
        match = True

        if msisdn and entry['msisdn'] != user['msisdn']:
            match = False

        if entity_type_name:
            if entity_type_name == "all":
                pass
            elif entity_type_name in ["BOUTIQUE", "LAB2.0", "FRANCHISE"] and entry["entity_type_name"] != entity_type_name:
                match = False

        if match:
            filtered_results.append(entry)

    return filtered_results

# Définir un modèle de données pour la réponse API
class Transaction(BaseModel):
    seller_id: str
    trnsaction_date: str
    offer_name: Optional[str]
    entity_name: str
    entity_type_name: str
    nombreactivation: int
    nombretransaction: int

# Variables globales pour stocker les données CSV
stored_data = []
stored_data_new = []

def read_csv(file_name: str, required_columns: List[str]) -> List[dict]:
    try:
        # Lire le fichier CSV en DataFrame pandas
        df = pd.read_csv(file_name)
        # Vérifier si les colonnes nécessaires existent
        for column in required_columns:
            if column not in df.columns:
                raise HTTPException(status_code=400, detail=f"Colonne manquante: {column}")
        # Convertir le DataFrame en dictionnaire
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur de lecture du fichier CSV: {str(e)}")

# Lire les fichiers CSV au démarrage de l'application
stored_data = read_csv("client_bi.csv", ["trnsaction_date", "activation_date", "nbr_transaction", "nbr_activation", "offer_name", "seller_id"])

@app.get("/get-data/")
async def get_data(
    tmcode: Optional[int] = Query(None, description="Code TM pour filtrer"),
    entity_name: Optional[str] = Query(None, description="Nom de l'entité pour filtrer"),
    activation_date: Optional[str] = Query(None, description="Date d'activation pour filtrer au format YYYY-MM-DD")
):
    if not stored_data:
        raise HTTPException(status_code=404, detail="Aucune donnée disponible")

    # Filtrer les données selon les paramètres de requête
    filtered_data = stored_data
    if tmcode is not None:
        filtered_data = [item for item in filtered_data if item["tmcode"] == tmcode]
    if entity_name is not None:
        filtered_data = [item for item in filtered_data if item["entity_name"] == entity_name]
    if activation_date is not None:
        filtered_data = [item for item in filtered_data if item["activation_date"] == activation_date]

    if not filtered_data:
        raise HTTPException(status_code=404, detail="Aucune donnée ne correspond aux critères de filtrage")
    
    # Trier les données par "trnsaction_date" puis par "activation_date"
    sorted_data = sorted(
        filtered_data,
        key=lambda x: (str(x["trnsaction_date"]), str(x["activation_date"])),
        reverse=True
    )

    # Ajouter le taux de conversion global à chaque ligne
    for item in sorted_data:
        nbr_transaction = item.get("nbr_transaction", 0)
        nbr_activation = item.get("nbr_activation", 0)
        item["taux_conversion_global"] = round(nbr_activation / nbr_transaction, 3) if nbr_transaction > 0 else 0.0
        item["taux_non_conversion_global"] = 1 - round(nbr_activation / nbr_transaction, 3) if nbr_transaction > 0 else 0.0

    # Remplacer les valeurs NaN et Inf par None pour éviter les erreurs de JSON
    sanitized_data = json.loads(json.dumps(sorted_data, allow_nan=False))

    return {"data": sanitized_data}



@app.get("/get-data-by-user/")
async def get_data_by_user(
    msisdn: str,
    password: str
):
    msisdn = authenticate_user(msisdn, password)
    if not stored_data:
        raise HTTPException(status_code=404, detail="Aucune donnée disponible")

    # Filtrer les données selon les paramètres de requête
    filtered_data = stored_data
    if msisdn['entity_name'] and 'all' not in msisdn['entity_name']:
        filtered_data = [item for item in filtered_data if item["entity_name"] in msisdn['entity_name']]

    if msisdn['entity_type_name'] != 'all':
        filtered_data = [item for item in filtered_data if item["entity_type_name"] == msisdn['entity_type_name']]

    if not filtered_data:
        raise HTTPException(status_code=404, detail="Aucune donnée ne correspond aux critères de filtrage")
    
    # Trier les données par "trnsaction_date" puis par "activation_date"
    sorted_data = sorted(
        filtered_data,
        key=lambda x: (str(x["trnsaction_date"]), str(x["activation_date"])),
        reverse=True
    )

    # Ajouter le taux de conversion global à chaque ligne
    for item in sorted_data:
        nbr_transaction = item.get("nbr_transaction", 0)
        nbr_activation = item.get("nbr_activation", 0)
        item["taux_conversion_global"] = round(nbr_activation / nbr_transaction, 3) if nbr_transaction > 0 else 0.0
        item["taux_non_conversion_global"] = 1 - round(nbr_activation / nbr_transaction, 3) if nbr_transaction > 0 else 0.0

    # Remplacer les valeurs NaN et Inf par None pour éviter les erreurs de JSON
    sanitized_data = []
    for item in sorted_data:
        sanitized_item = {}
        for k, v in item.items():
            if isinstance(v, (int, float)) and (pd.isna(v) or np.isinf(v)):
                sanitized_item[k] = None
            else:
                sanitized_item[k] = v
        sanitized_data.append(sanitized_item)

    return {"data": sanitized_data, "msisdn": msisdn}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
