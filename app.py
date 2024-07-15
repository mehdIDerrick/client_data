from fastapi import FastAPI, HTTPException, Query, Depends
from pydantic import BaseModel
from typing import List, Optional
import json
import pandas as pd
import uvicorn

# Charger les données JSON depuis le fichier
try:
    with open('Boutiques.json', 'r') as f:
        data = json.load(f)
except FileNotFoundError:
    print("Le fichier boutiques.json est introuvable.")
except json.JSONDecodeError:
    print("Erreur de déchiffrement JSON.")
except Exception as e:
    print(f"Une erreur est survenue : {str(e)}")



# Modèle Pydantic pour représenter les données JSON
class DataItem(BaseModel):
    msisdn: str
    refill: str
    entity_type_name: str
    entity_name: List[str]
    password: str

# Instanciation de l'application FastAPI
app = FastAPI()

# Fonction d'authentification
def authenticate_user(msisdn: str, password: str):
    for entry in data:
        if entry['msisdn'] == msisdn and entry['password'] == password:
            return entry
    raise HTTPException(status_code=401, detail="Unauthorized")

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

        if msisdn and entry['msisdn'] != msisdn['msisdn']:
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
    transaction_date: str
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
stored_data = read_csv("client.csv", ["trnsaction_date", "activation_date", "nbr_transaction", "nbr_activation", "offer_name", "seller_id"])
df_new = pd.read_csv('clients_new.csv')
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
        # Trier les données par "transaction_date" puis par "activation_date"
    sorted_data = sorted(
        filtered_data,
        key=lambda x: (x["trnsaction_date"], x["activation_date"]),
        reverse=True
    )
    # Ajouter le taux de conversion global à chaque ligne
    for item in sorted_data:
        nbr_transaction = item.get("nbr_transaction", 0)
        nbr_activation = item.get("nbr_activation", 0)
        item["taux_conversion_global"] = round(nbr_activation / nbr_transaction, 3) if nbr_transaction > 0 else 0.0
        item["taux_non_conversion_global"] = 1 - round(nbr_activation / nbr_transaction, 3) if nbr_transaction > 0 else 0.0
    return {"data": sorted_data}
@app.get("/get-data-by-user/")
async def get_data_by_user(
    msisdn: str,
    password: str
):
    user = authenticate_user(msisdn, password)
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
    
    # Trier les données par "transaction_date" puis par "activation_date"
    sorted_data = sorted(
        filtered_data,
        key=lambda x: (x["trnsaction_date"], x["activation_date"]),
        reverse=True
    )
    # Ajouter le taux de conversion global à chaque ligne
    for item in sorted_data:
        nbr_transaction = item.get("nbr_transaction", 0)
        nbr_activation = item.get("nbr_activation", 0)
        item["taux_conversion_global"] = round(nbr_activation / nbr_transaction, 3) if nbr_transaction > 0 else 0.0
        item["taux_non_conversion_global"] = 1 - round(nbr_activation / nbr_transaction, 3) if nbr_transaction > 0 else 0.0
    
    return {"data": sorted_data}

# Route pour récupérer toutes les transactions
@app.get("/get-data_all_new/", response_model=List[Transaction])
def get_all_transactions():
    # Remplacer NaN par une chaîne vide dans le DataFrame
    df_new_filled = df_new.fillna('')
    transactions = df_new_filled.to_dict(orient='records')
    return transactions

@app.get("/calculate_kpi/")
async def calculate_kpi():
    try:
        # Utiliser les données stockées
        data = stored_data

        # Créer un DataFrame à partir des données
        df = pd.DataFrame(data)

        # Afficher les premières lignes pour débogage
        print(df.head())

        # Calculer le nombre de transactions et d'activations par jour
        transactions_per_day = df.groupby('trnsaction_date').agg({'nbr_transaction': 'sum', 'nbr_activation': 'sum'}).reset_index()

        # Identifier les meilleurs vendeurs
        best_sellers = df.groupby(['seller_id','entity_name']).agg({'nbr_transaction': 'sum', 'nbr_activation': 'sum'}).reset_index()
        best_sellers = best_sellers.sort_values(by='nbr_transaction', ascending=False)

        # Convertir les résultats en dictionnaires pour JSON
        best_sellers_dict = best_sellers.to_dict(orient='records')

        # Retourner les résultats sous forme de JSON
        return {
            'best_sellers': best_sellers_dict
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/get_evolution/")
async def get_evolution():
    try:
        # Utiliser les données stockées
        data = stored_data

        # Créer un DataFrame à partir des données
        df = pd.DataFrame(data)

        # Afficher les premières lignes pour débogage
        print(df.head())

        # Calculer le nombre de transactions et d'activations par jour

        # Evolution par jour, offre, et vendeur
        evolution = df.groupby(['trnsaction_date', 'offer_name', 'seller_id','entity_name']).agg({
            'nbr_transaction': 'sum', 
            'nbr_activation': 'sum',
        }).reset_index()

        # Convertir les résultats en dictionnaires pour JSON
        evolution_dict = evolution.to_dict(orient='records')

        # Retourner les résultats sous forme de JSON
        return {
            'evolution': evolution_dict
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
