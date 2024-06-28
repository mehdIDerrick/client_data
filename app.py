from fastapi import FastAPI, HTTPException, Query
import pandas as pd
from typing import List, Optional
import uvicorn
from pydantic import BaseModel

app = FastAPI()
df_new = pd.read_csv('clients_new.csv')

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

    return {"data": filtered_data}

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
