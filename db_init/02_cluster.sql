/* Nejprve nastavíme, který index bude použit pro clustering */
ALTER TABLE Books CLUSTER ON id_book_author;
ALTER TABLE Ratings CLUSTER ON id_user_rating;
 
/* A pak ho opravdu provedeme */
CLUSTER Books;
CLUSTER Ratings;