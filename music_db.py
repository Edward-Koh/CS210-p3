import mysql.connector
from typing import List, Tuple, Set

class MusicDB:
    # open & close connection
    def __init__(self):
        self.conn = mysql.connector.connect(
            host='localhost', user='root', password='', database='eyk34_music_db' #<netid>_music_db for one group member
        )
        self.cur = self.conn.cursor()
    def close(self):
        self.conn.close()


    def clear_database(self): # make sure this is correct for autograder
        tables_in_order = [
            'ratings',
            'song_genres',
            'songs',
            'albums',
            'artists',
            'genres',
            'users'
        ]
        for x in tables_in_order:
            self.cur.execute(f"DELETE FROM {x}")
        self.conn.commit()

    def load_single_songs(self, singles: List[Tuple[str, Tuple[str, ...], str, str]]) -> Set[Tuple[str, str]]:
        """
        singles: list of tuples (artist_name, (genre1, genre2, ...), title, release_date)
        Inserts artists/genres if missing. Inserts songs with album_id=NULL and release_date provided.
        Returns set of (artist_name, song_title) that were added as new songs.
        """
        added = set()
        for artist, genres, title, date in singles:
            # ensure artist exists
            self.cur.execute("SELECT id FROM artists WHERE name = %s", (artist,))
            row = self.cur.fetchone()
            if row:
                artist_id = row[0]
            else:
                self.cur.execute("INSERT INTO artists (name) VALUES (%s)", (artist,))
                artist_id = self.cur.lastrowid

            # ensure genres exist and collect ids
            genre_ids = []
            for g in genres:
                self.cur.execute("SELECT id FROM genres WHERE name = %s", (g,))
                grow = self.cur.fetchone()
                if grow:
                    genre_ids.append(grow[0])
                else:
                    self.cur.execute("INSERT INTO genres (name) VALUES (%s)", (g,))
                    genre_ids.append(self.cur.lastrowid)
            if not genre_ids:
                continue

            # check if song already exists for this artist
            self.cur.execute("SELECT id FROM songs WHERE title = %s AND artist_id = %s", (title, artist_id))
            if not self.cur.fetchone():
                # insert single: album_id NULL
                self.cur.execute(
                    "INSERT INTO songs (title, artist_id, album_id, release_date) VALUES (%s, %s, NULL, %s)",
                    (title, artist_id, date)
                )
                song_id = self.cur.lastrowid
                for gid in genre_ids:
                    # use INSERT IGNORE in case duplicate (song_id, genre_id) already exists
                    self.cur.execute("INSERT IGNORE INTO song_genres (song_id, genre_id) VALUES (%s, %s)", (song_id, gid))
                added.add((artist, title))
            self.conn.commit()
        return added

    def load_albums(self, albums: List[Tuple[str, str, str, List[str]]]) -> Set[Tuple[str, str]]:
        """
        albums: list of tuples (artist_name, album_name, release_date, [song_title, ...])
        Inserts artists/genres/albums as needed. For each song in album, inserts it only if there's no song
        with the same title for that artist (prevents UNIQUE violation).
        Returns set of (artist_name, album_name) that were newly added as albums.
        """
        added = set()
        for artist, album_name, date, songs in albums:
            # ensure artist exists
            self.cur.execute("SELECT id FROM artists WHERE name = %s", (artist,))
            row = self.cur.fetchone()
            if row:
                artist_id = row[0]
            else:
                self.cur.execute("INSERT INTO artists (name) VALUES (%s)", (artist,))
                artist_id = self.cur.lastrowid

            #Determine album genre from first song(if it doesn't violate uniqueness)
            album_genre_id = None
            for title in songs:
                self.cur.execute("SELECT songs.id, song_genres.genre_id FROM songs "
                                "JOIN song_genres ON songs.id = song_genres.song_id "
                                "WHERE songs.title = %s AND songs.artist_id = %s", (title, artist_id))
                song_info = self.cur.fetchone()
                if song_info:
                    album_genre_id = song_info[1]
                    break
            
            #genre defaults to 'Unknown' if not found
            if album_genre_id is None:
                self.cur.execute("SELECT id FROM genres WHERE name = 'Unknown'")
                grow = self.cur.fetchone()
                if grow:
                    album_genre_id = grow[0]
                else:
                    self.cur.execute("INSERT INTO genres (name) VALUES ('Unknown')")
                    album_genre_id = self.cur.lastrowid

            #Check if album already exist for that artist
            self.cur.execute("SELECT id FROM albums WHERE name = %s AND artist_id = %s", (album_name, artist_id))
            arow = self.cur.fetchone()
            if arow:
                album_id = arow[0]
            else:
                self.cur.execute(
                    "INSERT INTO albums (name, artist_id, release_date, genre_id) VALUES (%s, %s, %s, %s)",
                    (album_name, artist_id, date, album_genre_id)
                )
                album_id = self.cur.lastrowid
                added.add((artist, album_name))

            #Insert songs with album_id
            for title in songs:
                self.cur.execute("SELECT id FROM songs WHERE title = %s AND artist_id = %s", (title, artist_id))
                if not self.cur.fetchone():
                    self.cur.execute("INSERT INTO songs (title, artist_id, album_id, release_date) VALUES (%s, %s, %s, %s)",
                                    (title, artist_id, album_id, date))
                    
                    song_id = self.cur.lastrowid
                    # Assign genre to song
                    self.cur.execute("INSERT IGNORE INTO song_genres (song_id, genre_id) VALUES (%s, %s)", (song_id, album_genre_id))
            self.conn.commit()
        return added

    def load_users(self, users: List[str]) -> Set[str]:
        rejected = set()
        for u in users:
            self.cur.execute("INSERT IGNORE INTO users (username) VALUES (%s)", (u,))
            if not self.cur.rowcount:
                rejected.add(u)
        self.conn.commit()
        return rejected

    def load_song_ratings(self, ratings: List[Tuple[str, Tuple[str, str], int, str]]) -> Set[Tuple[str, str, str]]:
        added = set()
        for user, (artist, title), stars, date in ratings:
            # get user id
            self.cur.execute("SELECT id FROM users WHERE username = %s", (user,))
            urow = self.cur.fetchone()
            if not urow:
                continue
            uid = urow[0]

            # get song id by joining artist -> songs
            self.cur.execute(
                "SELECT s.id FROM songs s JOIN artists a ON s.artist_id = a.id WHERE a.name = %s AND s.title = %s",
                (artist, title)
            )
            srow = self.cur.fetchone()
            if not srow:
                continue
            sid = srow[0]

            self.cur.execute(
                """INSERT INTO ratings (user_id, song_id, rating, rating_date)
                   VALUES (%s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE rating = VALUES(rating), rating_date = VALUES(rating_date)""",
                (uid, sid, stars, date)
            )
            added.add((user, artist, title))
            self.conn.commit()
        return added

    def get_most_prolific_individual_artists(self, n: int, yr: Tuple[int, int]) -> List[Tuple[str, int]]:
        s, e = yr
        self.cur.execute("""SELECT a.name, COUNT(*) FROM songs s JOIN artists a ON s.artist_id = a.id
                           WHERE YEAR(COALESCE(s.release_date, (SELECT release_date FROM albums WHERE id = s.album_id))) BETWEEN %s AND %s
                           GROUP BY a.id ORDER BY COUNT(*) DESC, a.name LIMIT %s""", (s, e, n))
        return self.cur.fetchall()

    def get_artists_last_single_in_year(self, year: int) -> Set[str]:
        self.cur.execute("""SELECT DISTINCT a.name FROM artists a JOIN songs s ON a.id = s.artist_id
                           WHERE s.album_id IS NULL AND YEAR(s.release_date) = %s
                           AND s.release_date = (SELECT MAX(s2.release_date) FROM songs s2
                                                 WHERE s2.artist_id = a.id AND s2.album_id IS NULL AND YEAR(s2.release_date) = %s)""",
                         (year, year))
        return {r[0] for r in self.cur.fetchall()}

    def get_top_song_genres(self, n: int) -> List[Tuple[str, int]]:
        self.cur.execute(
            "SELECT g.name, COUNT(*) FROM song_genres sg JOIN genres g ON sg.genre_id = g.id "
            "GROUP BY g.id ORDER BY COUNT(*) DESC, g.name LIMIT %s", (n,))
        return self.cur.fetchall()

    def get_album_and_single_artists(self) -> Set[str]:
        self.cur.execute("""SELECT a.name FROM artists a WHERE EXISTS
                           (SELECT 1 FROM songs WHERE artist_id = a.id AND album_id IS NOT NULL)
                           AND EXISTS (SELECT 1 FROM songs WHERE artist_id = a.id AND album_id IS NULL)""")
        return {r[0] for r in self.cur.fetchall()}

    def get_most_rated_songs(self, yr: Tuple[int, int], n: int) -> List[Tuple[str, str, int]]:
        s, e = yr
        self.cur.execute("""SELECT a.name, s.title, COUNT(*) FROM ratings r
                           JOIN songs s ON r.song_id = s.id JOIN artists a ON s.artist_id = a.id
                           WHERE YEAR(r.rating_date) BETWEEN %s AND %s
                           GROUP BY s.id ORDER BY COUNT(*) DESC, a.name, s.title LIMIT %s""", (s, e, n))
        return self.cur.fetchall()

    def get_most_engaged_users(self, yr: Tuple[int, int], n: int) -> List[Tuple[str, int]]:
        s, e = yr
        self.cur.execute("""SELECT u.username, COUNT(*) FROM ratings r JOIN users u ON r.user_id = u.id
                           WHERE YEAR(r.rating_date) BETWEEN %s AND %s
                           GROUP BY u.id ORDER BY COUNT(*) DESC, u.username LIMIT %s""", (s, e, n))
        return self.cur.fetchall()
