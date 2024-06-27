from fastapi import FastAPI, Query
from pydantic import BaseModel
import pandas as pd
from typing import List, Optional
import uvicorn
app = FastAPI()

# Charger les données CSV
df = pd.read_csv('client.csv')

# Définir un modèle de données pour la réponse API
class Transaction(BaseModel):
    seller_id: str
    transaction_date: str
    offer_name: Optional[str]
    entity_name: str
    entity_type_name: str
    nombreactivation: int
    nombretransaction: int

# Route pour récupérer toutes les transactions
@app.get("/get-data/", response_model=List[Transaction])
def get_all_transactions():
    transactions = df.to_dict(orient='records')
    return transactions

# Route pour filtrer les transactions
@app.get("/get-data/filter", response_model=List[Transaction])
def filter_transactions(
    seller_id: Optional[str] = Query(None),
    entity_name: Optional[str] = Query(None),
    entity_type_name: Optional[str] = Query(None),
    nombreactivation: Optional[int] = Query(None),
    nombretransaction: Optional[int] = Query(None)
):
    filtered_df = df.copy()

    if seller_id:
        filtered_df = filtered_df[filtered_df['seller_id'] == seller_id]
    if entity_name:
        filtered_df = filtered_df[filtered_df['entity_name'] == entity_name]
    if entity_type_name:
        filtered_df = filtered_df[filtered_df['entity_type_name'] == entity_type_name]
    if nombreactivation is not None:
        filtered_df = filtered_df[filtered_df['nombreactivation'] == nombreactivation]
    if nombretransaction is not None:
        filtered_df = filtered_df[filtered_df['nombretransaction'] == nombretransaction]

    transactions = filtered_df.to_dict(orient='records')
    return transactions

# Pour démarrer le serveur FastAPI
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
