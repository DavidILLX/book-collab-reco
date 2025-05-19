/* Vytvoření uživatele pro Flask API*/
CREATE USER flask_user WITH ENCRYPTED PASSWORD 'flask_pass';
GRANT CONNECT ON DATABASE database TO flask_user;
GRANT USAGE ON SCHEMA public TO flask_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO flask_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO flask_user;


/* Vytvoření tabulke při inicializaci*/
CREATE TABLE IF NOT EXISTS Books (
    "ISBN" VARCHAR(20) PRIMARY KEY,
    "Book-Title" TEXT NULL,
    "Book-Author" VARCHAR(255) NULL, 
    "Year-Of-Publication" INTEGER NULL,
    "Publisher" VARCHAR(255) NULL,
    "Image-URL-S" TEXT NULL,
    "Image-URL-M" TEXT NULL,
    "Image-URL-L" TEXT NULL
);
CREATE INDEX id_book_author ON Books("Book-Author");

CREATE TABLE IF NOT EXISTS Users (
    "User-ID" VARCHAR(20) PRIMARY KEY,
    "City" VARCHAR(255) NULL,
    "State" VARCHAR(255) NULL,
    "Country" VARCHAR(255) NULL, 
    "Age" FLOAT NULL
);

CREATE TABLE IF NOT EXISTS Ratings (
    "Ratings-ID" VARCHAR(50) PRIMARY KEY, 
    "User-ID" VARCHAR(20) NOT NULL, 
    "ISBN" VARCHAR(20) NULL,
    FOREIGN KEY ("User-ID") REFERENCES Users("User-ID"),
    FOREIGN KEY ("ISBN") REFERENCES Books("ISBN"),
    "Book-Rating" INTEGER NOT NULL
);
CREATE INDEX id_user_rating ON Ratings("User-ID");

