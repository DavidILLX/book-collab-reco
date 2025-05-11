from flask import Flask, flash, render_template, request, redirect, url_for
import os
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

# Zákaldní setup pro app a flash z Flask API
app = Flask(__name__)
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    print("Error ENV varible for Database is not set!")
    engine = None
else:
    while True:
        try:
            engine = create_engine(DATABASE_URL)
            conn = engine.connect()
            print("Connection to database successful!")
            break
        except Exception as error:
            print(f"Error when connecting to database: {error}")
            engine = None

# Načtení dat z databáze
books_data = pd.read_sql_table("books", conn)
ratings_data = pd.read_sql_table("ratings", conn)

conn.close()

def recommend_books(bookAuthor, bookTitle):
    # --- Logika z book_rec.py ---
    # Collaborative filtering na základě ostatních uživatelkých recenzí, kteří mají stejnou "chut" 
    bookAuthor = bookAuthor.lower()
    bookTitle = bookTitle.lower()

    # Načtení z databáze
    books_df = books_data.copy()

    ratings_df = ratings_data.copy()
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

    # Vybere jen ty knížky, které byly ohodnoceny 8 a více lidmi (Jak přišli na tuto hranici?)
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

        # Vypočítaní korelace knížky od uživatele s knížkou v datasetu
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

    # Převede výlsedky list listů s sloupci navíc pro více informací 
    columns_to_keep = ["ISBN", "Book-Title", "Book-Author"]
    full_book_info = []
    for title in results:
        full_df = books_df[books_df["Book-Title"] == title.title()]
        full_df = full_df[columns_to_keep]
        full_dic = full_df.to_dict(orient="records")
        full_book_info.extend(full_dic)
        
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

if __name__ == '__main__':
    app.run(debug=True)

   




