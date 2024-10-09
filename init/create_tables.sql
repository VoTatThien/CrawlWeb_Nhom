-- Tạo bảng dim_author
CREATE TABLE Author (
    author_id INT PRIMARY KEY,
    author VARCHAR(255)
);

CREATE TABLE Rating (
    book_id INT PRIMARY KEY,
    rating FLOAT,
    fivestars INT,
    fourstars INT,
    threestars INT,
    twostars INT,
    onestar INT
);


-- Tạo bảng book
CREATE TABLE Book (
    book_id INT PRIMARY KEY,
    rating_id INT,
    author_id INT,
    rating FLOAT,
    describe TEXT,
    author VARCHAR(255),
    bookname VARCHAR(255),
    publish VARCHAR(255),
    prices FLOAT,
    rating_count INT,
    reviews INT,
    pages_n INT,
    cover VARCHAR(255),
    bookUrl VARCHAR(255),
    fivestars INT,
    fourstars INT,
    threestars INT,
    twostars INT,
    onestar INT,
    FOREIGN KEY (rating_id) REFERENCES Rating(book_id),
    FOREIGN KEY (author_id) REFERENCES Author(author_id)
);



CREATE TABLE Describe (
    book_id INT,
    author_id INT,
    describe TEXT,
    bookUrl VARCHAR(255),
    PRIMARY KEY (book_id, author_id),
    FOREIGN KEY (book_id) REFERENCES Book(book_id),
    FOREIGN KEY (author_id) REFERENCES Author(author_id)
);



