import os
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests

load_dotenv()

def connect_mongo(uri): 
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    return client

def create_connect_db(client, db_name):
    db = client[db_name]
    
    return db

def create_connect_collection(db, col_name):
    collection = db[col_name]

    return collection

def  extract_api_data(url):
    response = requests.get(url)

    return response.json()

def  insert_data(col, data):
    docs = col.insert_many(data)
    return len(docs.inserted_ids)

if __name__=="__main__":
    uri = os.getenv("MONGODB_URI")
    url = "https://labdados.com/produtos"
    client = connect_mongo(uri)
    db = create_connect_db(client, "db_produtos_desafio")
    col = create_connect_collection(db, "produtos")
    data = extract_api_data(url)
    print(f"\nQuantidade de dados extraídos: {len(data)}")
    n_docs = insert_data(col, data)
    print(f"\nQuantidade de documentos inseridos: {n_docs}")
    client.close()