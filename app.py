from fastapi import FastAPI, HTTPException, Query
import pandas as pd
from typing import List, Optional
import uvicorn
app = FastAPI()

# Variable globale pour stocker les données CSV
stored_data = []

def read_csv():
    global stored_data
    try:
        # Lire le fichier CSV en DataFrame pandas
        df = pd.read_csv("client.csv")
        # Convertir le DataFrame en dictionnaire
        stored_data = df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur de lecture du fichier CSV: {str(e)}")

# Lire le fichier CSV au démarrage de l'application
read_csv()

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


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
