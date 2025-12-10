/* all based on music_db.pdf */
CREATE TABLE artists (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE genres (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE albums (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    artist_id INT NOT NULL,
    release_date DATE NOT NULL,
    genre_id INT NOT NULL,
    FOREIGN KEY (artist_id) REFERENCES artists(id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres(id),
    UNIQUE (name, artist_id)
);

CREATE TABLE songs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    artist_id INT NOT NULL,
    album_id INT DEFAULT NULL,
    release_date DATE DEFAULT NULL,
    FOREIGN KEY (artist_id) REFERENCES artists(id) ON DELETE CASCADE,
    FOREIGN KEY (album_id) REFERENCES albums(id) ON DELETE CASCADE,
    UNIQUE (artist_id, title),
    CHECK ((album_id IS NULL AND release_date IS NOT NULL) OR (album_id IS NOT NULL AND release_date IS NULL))
);

CREATE TABLE song_genres ( /* junction */
    song_id INT NOT NULL,
    genre_id INT NOT NULL,
    PRIMARY KEY (song_id, genre_id),
    FOREIGN KEY (song_id) REFERENCES songs(id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres(id) ON DELETE CASCADE
);

CREATE TABLE ratings (
    user_id INT NOT NULL,
    song_id INT NOT NULL,
    rating TINYINT UNSIGNED NOT NULL CHECK (rating BETWEEN 1 AND 5),
    rating_date DATE NOT NULL,
    PRIMARY KEY (user_id, song_id),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (song_id) REFERENCES songs(id) ON DELETE CASCADE
);

INSERT IGNORE INTO genres (name) VALUES ('Unknown');