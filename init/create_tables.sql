CREATE TABLE dim_author (
    author_id INTEGER PRIMARY KEY,
    author_name VARCHAR(255) NOT NULL
);

CREATE TABLE dim_book (
    book_id INTEGER PRIMARY KEY,
    bookname VARCHAR(255) NOT NULL,
    author_id INTEGER REFERENCES dim_author(author_id),
    prices DECIMAL(5, 2),
    describe TEXT,
    pages_n INTEGER,
    cover VARCHAR(50),
    publish DATE
);

CREATE TABLE fact_book_ratings (
    book_id INTEGER REFERENCES dim_book(book_id),
    rating FLOAT,
    ratingcount INTEGER,
    reviews INTEGER,
    fivestars INTEGER,
    fourstars INTEGER,
    threestars INTEGER,
    twostars INTEGER,
    onestar INTEGER,
    PRIMARY KEY (book_id)
);
