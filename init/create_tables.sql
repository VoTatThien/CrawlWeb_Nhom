-- Tạo bảng dim_author
CREATE TABLE Author (
    id_author SERIAL PRIMARY KEY,
    author_name VARCHAR(255)
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
    book_id SERIAL PRIMARY KEY,
    rating_id INT,
    id_author INT,
    rating FLOAT,
    description TEXT,
    author_name VARCHAR(255),
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
    FOREIGN KEY (id_author) REFERENCES Author(id_author)
);



CREATE TABLE Describe (
    book_id INT,
    id_author INT,
    description TEXT,
    bookUrl VARCHAR(255),
    PRIMARY KEY (book_id, id_author),
    FOREIGN KEY (book_id) REFERENCES Book(book_id),
    FOREIGN KEY (id_author) REFERENCES Author(id_author)
);



