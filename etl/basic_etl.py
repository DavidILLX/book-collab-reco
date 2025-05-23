import pandas as pd
import numpy as np
from kaggle.api.kaggle_api_extended import KaggleApi
from datetime import date
import os
import logging
from sqlalchemy import create_engine
import time
import hashlib   

def download_data():
    # Vytvoří adresář, pokud neexistuje
    directory_name = "data"

    try:
        os.mkdir(directory_name)
        logging.info(f"Directory '{directory_name}' created successfully.")
    except FileExistsError:
        logging.warning(f"Directory '{directory_name}' already exists.")
    except PermissionError:
        logging.error(f"Permission denied: Unable to create '{directory_name}'.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

    # Smaže soubory pokud ve složce nějaké jsou
    for filename in os.listdir(directory_name):
        file_path = os.path.join(directory_name, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)  

    # Kaggle API pro stažení data setu s knihama
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files("arashnic/book-recommendation-dataset", path="data/", unzip=True)

def read_data():
    download_data()

    books_df = None
    users_df = None
    ratings_df = None

    # Pokus o načtení books
    try:
        books_df = pd.read_csv("data/Books.csv", sep=",", encoding="utf-8")
        logging.info(f"File Books.csv loaded. Num of rows: {len(books_df)}")
    except FileNotFoundError:
        logging.error("Books.csv not found.")
    except Exception as e:
        logging.error(f"Error while loading Books.csv: {e}")

    # Pokus o načtení users
    try:
        users_df = pd.read_csv("data/Users.csv")
        logging.info(f"File Users.csv loaded. Num of rows: {len(users_df)}")
    except FileNotFoundError:
        logging.error("Users.csv not found.")
    except Exception as e:
        logging.error(f"Error while loading Users.csv: {e}")


    # Pokus o načtení ratings
    try:
        ratings_df = pd.read_csv("data/Ratings.csv")
        logging.info(f"File Ratings.csv loaded. Num of rows: {len(ratings_df)}")
    except FileNotFoundError:
        logging.error("Ratings.csv not found.")
    except Exception as e:
        logging.error(f"Error while loading Ratings.csv: {e}")

    return books_df, users_df, ratings_df

def fix_special_characters(text):

    # Mapa pro opravy specifických znaků (provizorní, je třeba mít dostatečně dobrá data)
    char_map = {
        # Latin-1/CP1252 znaky čtené jako UTF-8
        'Ã©': 'é',
        'Ã¨': 'è',
        'Ã­': 'í',
        'Ã§': 'ç',
        'Ã³': 'ó',
        'Ã¡': 'á',
        'Ã´': 'ô',
        'Ã¼': 'ü',
        'Ã¶': 'ö',
        'Ã½': 'ý',
        'Ã£': 'ã',
        'Ãº': 'ú',

        # Latin-1/CP1252
        'Ã?Â¤': 'ä', 
        'Ã?Â©': 'é', 
        'Ã?Â¼': 'ü', 
        'Ã?Â¶': 'ö', 
        'Ã?Â?': 'Ü', 
        'Ã?Â?': 'Ä', 
        'Ã?Â?': 'Ö', 
        'Ã?Â?': 'É', 
        'Ã?Â?': 'Á', 
        'Ã?Â?': 'Í', 
        'Ã?ÂŸ': 'ß', 

        # Španělština
        'Â¿': '¿', 
        'Â¡': '¡',

        # Čeština
        'ÄŤ': 'č',
        'Å™': 'ř',
        'Å¡': 'š',
        'Å¾': 'ž',
        'Ãº': 'ú', 

        # vypadá to, že 'Ã? ' by mělo být 'Il '
        'Ã? ': 'Il ',
    }

    if isinstance(text, str):  # Zkontroluj, že text je řetězec
        for wrong, correct in char_map.items():
            text = text.replace(wrong, correct)
    return text

def extract_author_from_title(books_df: pd.DataFrame) -> pd.DataFrame:

    mask_wrong_title = books_df["Book-Title"].str.contains(r'\\";')
    index_to_fix = books_df[mask_wrong_title].index
    author_columns = books_df.columns[books_df.columns.get_loc('Book-Author'):].tolist()

    for index in index_to_fix:
        problematic_title = books_df.loc[index, 'Book-Title']

        # Split podle '\";'
        parts = problematic_title.split('\\";', 1)

        if len(parts) == 2:
            correct_title = parts[0]
            # Část po '\";' je začátek dat, která se posunula doleva
            # První prvek v 'data_to_shift_left' by měl být správný autor
            start_of_shifted_data = parts[1]

            # Získáme hodnoty ze sloupců, které jsou aktuálně posunuté, začínáme od 'Book-Author' a posunití vpravo
            current_shifted_values = books_df.loc[index, author_columns].tolist()

            # První hodnota bude to, co následovalo po '\";' (správný autor)
            # Následují hodnoty z 'current_shifted_values', posunuté doleva
            correctly_ordered_values = [start_of_shifted_data] + current_shifted_values[:-1]

            # Přiřadíme opravený název knihy
            books_df.loc[index, 'Book-Title'] = correct_title

            # Přiřadíme opravené a posunuté hodnoty do sloupců od 'Book-Author' dál
            books_df.loc[index, author_columns] = correctly_ordered_values
            books_df['Book-Title'] = books_df['Book-Title'].str.strip('"')

    return books_df

def remove_special_scharacters(df: pd.DataFrame) -> pd.DataFrame:
    
    # Odstranění pozůstatků z web scrapingu
    columns_to_change = df.select_dtypes(include=["object"]).columns
    for col in columns_to_change:
        df[col] = df[col].str.replace(r'\\"', '"', regex=True)
        df[col] = df[col].str.replace(r'\\', '"', regex=True)
        df[col] = df[col].str.replace('&amp;', ' & ', regex=False)
        df[col] = df[col].str.strip('"')

    return books_df

def fix_publishing_year(books_df: pd.DataFrame) -> pd.DataFrame:

    books_df["Year-Of-Publication"] = pd.to_numeric(books_df["Year-Of-Publication"], errors="coerce")

    date_now = date.today()

    # Některé roky vvydaní jsou nula nebo v budoucnosti i když podle Amazonu mají mít jině 
    mask_for_years = (books_df["Year-Of-Publication"] == 0) | (books_df["Year-Of-Publication"] > date_now.year) | \
    (books_df["Year-Of-Publication"] == 1376) | (books_df["Year-Of-Publication"] == 1378) 

    # NaN pokud spadá do jedné z podmínek výše
    books_df.loc[mask_for_years, "Year-Of-Publication"] = np.nan

    # Revast z float na integer
    books_df["Year-Of-Publication"] = books_df["Year-Of-Publication"].astype("Int64")
    return books_df

def ratings_preprocessing(ratings_df: pd.DataFrame, books_df: pd.DataFrame) -> pd.DataFrame:
    # Doplnění unikátního hashe pro databázi, detekce duplicit
    hash_id = ratings_df["User-ID"].astype(str) + "_" + ratings_df["ISBN"].astype(str) 
    ratings_df["Ratings-ID"] = hash_id.map(lambda x: hashlib.md5(x.encode()).hexdigest())
    ratings_df["Ratings-ID"].drop_duplicates()

    # Přesun sloupce s hash(Rating-ID) na začátek
    ratings_df = ratings_df.iloc[:,[3, 0, 1, 2]]

    # Odstranění ratings pro které nejsou knihy v books (Pokud se později ty knihy nepřidají)
    valid_isbn = set(books_df["ISBN"].dropna())
    ratings_df = ratings_df[ratings_df["ISBN"].isin(valid_isbn)]
    
    return ratings_df

def text_basic_preprocessing(df: pd.DataFrame) -> pd.DataFrame:
    # Obyčejní text preprocessing pro normaliziaci

    object_columns = df.select_dtypes(include=["object"]).columns
    filtered_columns = object_columns[~object_columns.str.contains("URL|ISBN", regex=True)]

    df.loc[:, filtered_columns] = df[filtered_columns].apply(lambda col: col.str.strip().str.title())

    return df

def users_preprocessing(users_df: pd.DataFrame) -> pd.DataFrame:

    # Rozdělení lokace pro normalizaci dat v databázi
    location_df = users_df["Location"].str.split(",", expand=True, n=2)
    location_df.rename(columns={0: "City", 1: "State", 2: "Country"},  inplace=True)

    # Spojení datasetu a přerovnání sloupců pro přehlednost
    users_df = pd.concat([users_df,location_df], axis=1)
    users_df = users_df.drop(columns="Location")
    users_df = users_df.iloc[:,[0, 2, 3, 4, 1]]

    # Odstranění duplicit
    users_df = users_df.drop_duplicates(subset = "User-ID")

    return users_df

def repair_encoding(books_df) -> pd.DataFrame:

    # Aplikuj funkci na sloupce s textovými daty
    columns_to_change = ['Book-Title', 'Book-Author', 'Publisher']
    for col in columns_to_change:
        books_df[col] = books_df[col].apply(fix_special_characters)

    return books_df

def connect_to_db():
    DATABASE_URL = os.getenv("DATABASE_URL")

    while True:
        try:
            engine = create_engine(DATABASE_URL)
            conn = engine.connect()
            logging.info("Connection to database successful!")
            return conn
        except Exception as error:
            logging.error(f"Error when connecting to database: {error}")
            time.sleep(3)

def insert_into_db(users_df: pd.DataFrame, ratings_df: pd.DataFrame, books_df: pd.DataFrame):

    conn = connect_to_db()
    try:
        #INSERT INTO PostgreSQL
        users_df.to_sql("users", con=conn, schema=None, if_exists="append", index=False)
        books_df.to_sql("books", con=conn, schema=None, if_exists="append", index=False)
        ratings_df.to_sql("ratings", con=conn, schema=None, if_exists="append", index=False)
    except Exception as error:
        logging.error(f"Error when inserting rows. Error: {error}")

    conn.close()
    
    logging.info("Succesfully inserted records!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format ="%(asctime)s/%(levelname)s/%(message)s", force=True)
    logging.info("Running ETL...")

    # --- 0. Retry mechanismus pro databázi ---
    connect_to_db()

    # --- 1. Načtení dat ---
    books_df, users_df, ratings_df = read_data()

    # --- 2. Čištění a preprocessing dat ---
    books_df = extract_author_from_title(books_df)
    books_df = fix_publishing_year(books_df)
    books_df = remove_special_scharacters(books_df)
    books_df = repair_encoding(books_df)
    books_df = text_basic_preprocessing(books_df)
    logging.info("Cleaning Books data done")

    users_df = users_preprocessing(users_df) 
    users_df = text_basic_preprocessing(users_df)
    logging.info("Cleaning Users data done")

    ratings_df = ratings_preprocessing(ratings_df, books_df)
    logging.info("Cleaning Rratings data done")

    # --- 3. Nahrání dat do databáze ---
    insert_into_db(users_df, ratings_df, books_df)
