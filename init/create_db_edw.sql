CREATE DATABASE ewd_db;

\c ewd_db


CREATE TABLE book_ewd (
    book_id INT PRIMARY KEY,
    author_id INT,
    rating  DECIMAL(5, 2),
    genre VARCHAR(255),
    describe TEXT,
    author VARCHAR(255),
    bookname VARCHAR(255),
    publish Date,
    prices  DECIMAL(5, 2),
    ratingcount INT,
    quantity  INT,
    pages_n INT,
    cover VARCHAR(255)
);