from flask import Flask, flash, render_template, request, redirect, url_for
import os
import logging
from sqlalchemy import create_engine, Column, String, Float, Integer, ForeignKey
from sqlalchemy.orm import declarative_base
import re
import pandas as pd
import numpy as np
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s/%(levelname)s/%(message)s", force=True)

# Zákaldní setup pro app a flash z Flask API
app = Flask(__name__)
app.secret_key = os.urandom(24)

# Globání proměnné
books_data = None
ratings_data = None

# Deklarování modelů pro ORM
base = declarative_base()

class Users(base):
    __tablename__ = "users"
    user_id = Column(String, primary_key = True)
    city = Column(String, nullable = True)
    state = Column(String, nullable = True)
    country = Column(String, nullable = True)
    age = Column(Float, nullable = True)

class Books(base):
    __tablename__ = "books"
    isbn = Column(String, primary_key = True)
    book_title = Column(String, nullable = True)
    book_author = Column(String, nullable = True)
    year_of_publication = Column(Integer, nullable= True)
    publisher = Column(String, nullable = True)
    image_url_s = Column(String, nullable = True)
    image_url_m = Column(String, nullable = True)
    image_url_l = Column(String, nullable = True)

class Ratings(base):
    __tablename__ = "ratings"

    ratings_id = Column(Integer, primary_key = True)
    user_id = Column(String, ForeignKey("users.user_id"))
    isbn  = Column(String, ForeignKey("books.isbn"))
    rating = Column(Integer, nullable = False)

def get_data():
    global books_data, ratings_data

    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        logging.error("Error ENV varible for Database is not set!")
        engine = None
    else:
        while True:
            try:
                engine = create_engine(DATABASE_URL)
                conn = engine.connect()
                logging.info("Connection to database successful!")
                
                books_data = pd.read_sql_table("books", conn)
                ratings_data = pd.read_sql_table("ratings", conn)

                if not books_data.empty and not ratings_data.empty:
                    logging.info("Books and ratings data successfully loaded!")
                    break
                else:
                    logging.warning("Tables not found in the database.")

            except Exception as error:
                logging.error(f"Error when connecting to database: {error}")
                engine = None
                time.sleep(3)
    conn.close()
    return books_data, ratings_data

# Dotazy na db podle Knihy a autora, mělo by zlepšit čas načtení dat
def get_data_ORM(bookAuthor, bookTitle):
    # Přdělení první částí recoommend books, pro získání jen určitých ratings a book ńa zákaldě knihy a autora
    # Následuje stejnou logiku ale mělo by být rychlejší 
    author_filtered = Books.book_author.ilike(f"%{bookAuthor}") if bookAuthor else True # Shoda s čimkoliv
    readers_query = conn.query(Ratings.user_id).distinct().join(Books).filter(
       Books.book_title.ilike(bookTitle), author_filtered
    )
    results_readers = readers_query.all()
    authors_results = [row[0] for row in results_readers]

    ratings_query = conn.query(
        Ratings.user_id,
        Ratings.isbn,
        Ratings.rating,
        Books.book_title
    ).join(Books).filter(
        Ratings.user_id.in_(authors_results),
        Ratings.rating != 0
    )

    results_rating_query = ratings_query.all()
    merged_dataset = pd.DataFrame(results_rating_query)

    return merged_dataset

def recommend_books(bookAuthor, bookTitle):
    global books_data, ratings_data

    # --- Logika z book_rec.py ---
    # Collaborative filtering na základě ostatních uživatelkých recenzí, kteří mají stejnou "chut" 
    # Očištění inputů od uživatele whitespace, malá písmena a speciální znaky
    bookAuthor = bookAuthor.lower().strip()
    bookTitle = bookTitle.lower().strip()
    
    bookAuthor = re.sub(r"[!@#$%^\"]", "", bookAuthor)
    bookTitle = re.sub(r"[!@#$%^\"]", "", bookTitle)

    if books_data is None or ratings_data is None:
        books_data, ratings_data = get_data()

    # Načtení z databáze
    books_df = books_data

    ratings_df = ratings_data
    ratings_df = ratings_df[ratings_df["Book-Rating"] != 0] 

    #users_df = pd.read_sql_table("users", conn)

    # Spojení datasetů na základě ISBN ratings a books
    #merged_ru_dataset = pd.merge(ratings, users, on=['User-ID'])
    merged_rb_dataset = pd.merge(ratings_df, books_df, on=["ISBN"])
    merged_rb_dataset = merged_rb_dataset.apply(lambda x: x.str.lower() if(x.dtype == "object") else x)

    # Vezme všechny usery, kteří četli stejnou knížku od stejného autora a nechá jen unikátní záznamy
    author_readers = merged_rb_dataset["User-ID"][(merged_rb_dataset["Book-Title"] == bookTitle) & (merged_rb_dataset["Book-Author"].str.contains(bookAuthor))]
    author_readers = author_readers.tolist()
    author_readers = np.unique(author_readers)

    # Ponechá jen uživatelé, kteří četli stejnou knížku a nechá si jen jejich knížky. Vytvoření něco jako "podobných profilů" 
    # final dataset
    books_of_author_readers = merged_rb_dataset[(merged_rb_dataset["User-ID"].isin(author_readers))]

    # Počet kolik dostala každá knížka rating od uživatelů se stejným profilem
    # Number of ratings per other books in dataset
    number_of_rating_per_book = books_of_author_readers.groupby(["Book-Title"]).agg("count").reset_index()

    # Vybere jen ty knížky, které byly ohodnoceny 8 a více lidmi (Jak přišli na tuto hranici?, zaměnil jsem za 5 pro testování)
    #select only books which have actually higher number of ratings than threshold
    books_to_compare = number_of_rating_per_book["Book-Title"][number_of_rating_per_book["User-ID"] >= 5]
    books_to_compare = books_to_compare.tolist()

    # Dataset s každým ratingem od uživatele podle knížek které byly hodnoceny 8 a více lidmi 
    ratings_data_raw = books_of_author_readers[["User-ID", "Book-Rating", "Book-Title"]][books_of_author_readers["Book-Title"].isin(books_to_compare)]

    # Vypočte průměrný rating pro každou knížku od každého uživatele? 
    # Nestačí zde jen Book_Title abysme dostali průmerné hodnocení od lidi se stejným profilem.
    # group by User and Book and compute mean
    ratings_data_raw_nodup = ratings_data_raw.groupby(["User-ID", "Book-Title"])["Book-Rating"].mean()

    # reset index to see User-ID in every row
    ratings_data_raw_nodup = ratings_data_raw_nodup.to_frame().reset_index()

    # Vytvoření dataframu kde každý index je uživatel, sloupec kniha a Book-rating od každého uživatele ke knize
    dataset_for_corr = ratings_data_raw_nodup.pivot(index="User-ID", columns="Book-Title", values="Book-Rating")

    users_book = [bookTitle]

    result_list = []
    worst_list = []

    for books in users_book:

        if books in dataset_for_corr.columns:
            # Odstranění uživatelovi oblíbené knížky z dataset. Jasné, že by měla tu největší shodu.
            dataset_of_other_books = dataset_for_corr.copy(deep=False)
            dataset_of_other_books.drop([books], axis=1, inplace=True)
        else:
            dataset_of_other_books = dataset_for_corr.copy(deep=False)
        
        # Inicializace prázdných listů pro doplnění knich, korelační matice a průměrného ratingu
        # empty lists
        book_titles = []
        correlations = []
        avg_rating = []

        # Vypočítaní korelace knížky od uživatele s knížkou v datasetu skrze všechny knížky 
        # corr computation
        for book_title in list(dataset_of_other_books.columns.values):
            book_titles.append(book_title)
            correlations.append(dataset_for_corr[books].corr(dataset_of_other_books[book_title]))
            tab = (ratings_data_raw[ratings_data_raw["Book-Title"] == book_title].groupby(ratings_data_raw["Book-Title"])) # zde byl mean() na Book-Title?
            avg_rating.append(tab["Book-Rating"].mean()) # Přídá nejmeněí růměrný rating jako avg_rating? (předpokládám zde mean())

        # Vyvoření dataframu z dříve init listů
        # final dataframe of all correlation of each book   
        corr_df = pd.DataFrame(list(zip(book_titles, correlations, avg_rating)), columns=["Book","Correlation","Avg_rating"])

        # Uloží 10 knih s největší korelací (head)
        # top 10 books with highest corr
        result_list.append(corr_df.sort_values("Correlation", ascending = False).head(10))
        
        # Uloží 10 knížek s nejnižší korelací (tail), nejsou sice potřeba, ale budiž
        # worst 10 books
        worst_list.append(corr_df.sort_values("Correlation", ascending = False).tail(10))

    results = result_list[0]["Book"].tolist()

    # Převede výlsedky na dictioniry (k:v) navíc pro více informací, uloží pouze první match s knihou
    # V logice se knihy agregují podle Book-Title a Book-Author, takže také neřeší duplikace pro jiné ISBN 
    # Může být jiný vítisk lépe hodnocený -> lepší překlad, ilustrace hardback/softback apod.
    columns_to_keep = ["Book-Title", "Book-Author"]
    full_book_info = []
    seen_titles = set()

    for title in results:

        title_lower = title.lower()
        matched_books = books_df[books_df["Book-Title"].str.lower() == title_lower]
    
        if title_lower in seen_titles or matched_books.empty:
            continue

        first_match = matched_books[columns_to_keep].iloc[0].to_dict()
        full_book_info.append(first_match)
        seen_titles.add(title_lower)
        
    results = full_book_info
    return results

@app.route("/", methods=["POST", "GET"])
def index():

    if request.method == "POST":
        bookAuthor = request.form.get("bookAuthor")
        bookTitle = request.form.get("bookTitle")

        results = recommend_books(bookAuthor, bookTitle)

        # Kontrola jestli nejsou results prázdné, pokud ano flask flash msg 0 Matches
        if len(results) == 0:
            flash("0 Matches :(")
            return redirect(url_for("index"))
        else:
            return render_template("index.html", results = results)
    
    return render_template("index.html", results = None)

@app.route("/reload", methods=["POST", "GET"])
def reload_data():
    global books_data, ratings_data
    try:
        books_data, ratings_data = get_data()
        if books_data is not None and ratings_data is not None:
            logging.info("Data succesfully reloaded")
            return redirect(url_for("index"))
        else:
            logging.warning("Data not reloaded!")
            return redirect(url_for("index"))
    except Exception as error:
        logging.error(f"Error: {error}")
        return redirect(url_for("index"))

if __name__ == '__main__':
    app.run(debug=True)
