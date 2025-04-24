from a_extract_and_save_data import connect_mongo, create_connect_db, create_connect_collection
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

def visualize_collection(col):
    for doc in col.find():
        print(doc)


def rename_column(col, col_name, new_name):
    col.update_many({}, {"$rename": {f"{col_name}": f"{new_name}"}})


def select_category(col, category):
    query = {"Categoria do Produto": f"{category}"}
    lista = []
    for doc in col.find(query):
        lista.append(doc)
    return lista

def make_regex(col, regex):
    query = {"Data da Compra": {"$regex": f"{regex}"}}
    lista_regex = []
    for doc in col.find(query):
        lista_regex.append(doc)
    return lista_regex


def create_dataframe(lista):
    df_livros = pd.DataFrame(lista)
    return df_livros

def format_date(df):
    df["Data da Compra"] = pd.to_datetime(df["Data da Compra"], format="%d/%m/%Y")
    df["Data da Compra"] = df["Data da Compra"].dt.strftime("%Y-%m-%d")
 

def save_csv(df, path):
    df.to_csv(path, index=False)
    print(f"\nO arquivo {path} foi salvo")

if __name__=="__main__":
    uri = os.getenv("MONGODB_URI")
    client = connect_mongo(uri)
    db = create_connect_db(client, "db_produtos_desafio")
    col = create_connect_collection(db, "produtos")
    
    rename_column(col, "lat", "Latitude")
    rename_column(col, "lon", "Longitude")
    
    livros = select_category(col, "livros")
    df_livros = create_dataframe(livros)
    format_date(df_livros)
    save_csv(df_livros, "../data/tb_livros.csv")

    regex = "/202[1-9]"
    lst_produtos = make_regex(col, regex)
    df_produtos = create_dataframe(lst_produtos)
    format_date(df_produtos)
    save_csv(df_produtos, "../data/tb_produtos.csv")