import mysql.connector
from typing import List, Tuple, Set

class MusicDB:
    # open & close connection
    def __init__(self):
        self.conn = mysql.connector.connect(
            host='localhost', user='root', password='', database='music_db' #<netid>_music_db for one group member
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
        Add single songs to the database. 

        Args:
            mydb: database connection
            
            single_songs: List of single songs to add. Each single song is a tuple of the form:
                (song title, genre names, artist name, release date)
            Genre names is a tuple since a song could belong to multiple genres
            Release date is of the form yyyy-dd-mm
            Example 1 single song: ('S1',('Pop',),'A1','2008-10-01') => here song is of genre Pop
            Example 2 single song: ('S2',('Rock', 'Pop),'A2','2000-02-15') => here song is of genre Rock and Pop

        Returns:
            Set[Tuple[str,str]]: set of (song,artist) for combinations that already exist 
            in the database and were not added (rejected). 
            Set is empty if there are no rejects.
        """
        rejected = set()
        for item in singles:
            title, genres, artist, date = item
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

            self.cur.execute("SELECT id FROM songs WHERE title = %s AND artist_id = %s", (title, artist_id))
            if self.cur.fetchone():
                rejected.add((title, artist))
                continue

            #Insert single (album_id NULL)
            self.cur.execute("INSERT INTO songs (title, artist_id, album_id, release_date) VALUES (%s, %s, NULL, %s)",
                            (title, artist_id, date))
            song_id = self.cur.lastrowid

            #Link song to genres
            for gid in genre_ids:
                self.cur.execute("INSERT IGNORE INTO song_genres (song_id, genre_id) VALUES (%s, %s)",
                                (song_id, gid))
            self.conn.commit()
        return rejected

    def load_albums(self, albums: List[Tuple[str, str, str, List[str]]]) -> Set[Tuple[str, str]]:
        """
        Add albums to the database. 
        
        Args:
            mydb: database connection
            
            albums: List of albums to add. Each album is a tuple of the form:
                (album title, genre, artist name, release date, list of song titles) 
            Release date is of the form yyyy-dd-mm
            Example album: ('Album1','Jazz','A1','2008-10-01',['s1','s2','s3','s4','s5','s6'])

        Returns:
            Set[Tuple[str,str]: set of (album, artist) combinations that were not added (rejected) 
            because the artist already has an album of the same title.
            Set is empty if there are no rejects.
        """
        rejected = set()
        for item in albums:
            # ensure artist exists
            album_title, album_genre, artist, release_date, songs = item

            self.cur.execute("SELECT id FROM artists WHERE name = %s", (artist,))
            row = self.cur.fetchone()
            if row:
                artist_id = row[0]
            else:
                self.cur.execute("INSERT INTO artists (name) VALUES (%s)", (artist,))
                artist_id = self.cur.lastrowid

            #add given album genre
            self.cur.execute("SELECT id FROM genres WHERE name = %s", (album_genre,))
            grow = self.cur.fetchone()
            if grow:
                album_genre_id = grow[0]
            else:
                self.cur.execute("INSERT INTO genres (name) VALUES (%s)", (album_genre,))
                album_genre_id = self.cur.lastrowid

            #Check if album already exists for artist
            self.cur.execute(
                "SELECT id FROM albums WHERE name = %s AND artist_id = %s",
                (album_title, artist_id)
            )
            arow = self.cur.fetchone()

            if arow:
                rejected.add((album_title, artist))
                continue

            self.cur.execute(
                "INSERT INTO albums (name, artist_id, release_date, genre_id) VALUES (%s, %s, %s, %s)",
                (album_title, artist_id, release_date, album_genre_id)
            )
            album_id = self.cur.lastrowid

            #Check if album already exist for that artist
            for title in songs:
            # Insert only if title does not already exist for this artist
                self.cur.execute("SELECT id FROM songs WHERE title = %s AND artist_id = %s",
                                (title, artist_id))
                if not self.cur.fetchone():
                    self.cur.execute("INSERT INTO songs (title, artist_id, album_id, release_date) VALUES (%s, %s, %s, %s)",
                                    (title, artist_id, album_id, release_date))
                    song_id = self.cur.lastrowid

                    # Assign this album’s genre to every song
                    self.cur.execute("INSERT INTO song_genres (song_id, genre_id) VALUES (%s, %s)",
                                    (song_id, album_genre_id))
            self.conn.commit()
        return rejected

    def load_users(self, users: List[str]) -> Set[str]:
        """
        Add users to the database. 

        Args:
            mydb: database connection
            users: list of usernames

        Returns:
            Set[str]: set of all usernames that were not added (rejected) because 
            they are duplicates of existing users.
            Set is empty if there are no rejects.
        """
        rejected = set()
        for u in users:
            self.cur.execute("INSERT IGNORE INTO users (username) VALUES (%s)", (u,))
            if not self.cur.rowcount:
                rejected.add(u)
        self.conn.commit()
        return rejected

    def load_song_ratings(self, ratings: List[Tuple[str, Tuple[str, str], int, str]]) -> Set[Tuple[str, str, str]]:
        """
        Load ratings for songs, which are either singles or songs in albums. 

        Args:
            mydb: database connection
            song_ratings: list of rating tuples of the form:
                (rater, (artist, song), rating, date)
            
            The rater is a username, the (artist,song) tuple refers to the uniquely identifiable song to be rated.
            e.g. ('u1',('a1','song1'),4,'2021-11-18') => u1 is giving a rating of 4 to the (a1,song1) song.

        Returns:
            Set[Tuple[str,str,str]]: set of (username,artist,song) tuples that are rejected, for any of the following
            reasongs:
            (a) username (rater) is not in the database, or
            (b) username is in database but (artist,song) combination is not in the database, or
            (c) username has already rated (artist,song) combination, or
            (d) everything else is legit, but rating is not in range 1..5
            
            An empty set is returned if there are no rejects.  
        """

        rejected = set()
        for user, (artist, title), stars, date in ratings:
            #(a)check if user exist
            self.cur.execute("SELECT id FROM users WHERE username = %s", (user,))
            urow = self.cur.fetchone()
            if not urow:
                rejected.add((user, artist, title))
                continue
            uid = urow[0]

            #(b) check if song exist (artist, title)
            self.cur.execute("SELECT s.id FROM songs s JOIN artists a ON s.artist_id = a.id WHERE a.name = %s AND s.title = %s",
                            (artist, title))
            srow = self.cur.fetchone()
            if not srow:
                rejected.add((user, artist, title))
                continue
            sid = srow[0]

            #(d) check if ratings are good
            try:
                r = int(stars)
            except Exception:
                rejected.add((user, artist, title))
                continue
            if r < 1 or r > 5:
                rejected.add((user, artist, title))
                continue

            #(c)check if user already rated this song
            self.cur.execute("SELECT 1 FROM ratings WHERE user_id = %s AND song_id = %s",
                            (uid, sid))
            if self.cur.fetchone():
                rejected.add((user, artist, title))
                continue

            #Insert rating
            self.cur.execute("INSERT INTO ratings (user_id, song_id, rating, rating_date) VALUES (%s, %s, %s, %s)",
                            (uid, sid, r, date))
            self.conn.commit()
        return rejected

    #queries
    def get_most_prolific_individual_artists(self, n: int, yr: Tuple[int, int]) -> List[Tuple[str, int]]:
        """
        Get the top n most prolific individual artists by number of singles released in a year range. 
        Break ties by alphabetical order of artist name.

        Args:
            mydb: database connection
            n: how many to get
            year_range: tuple, e.g. (2015,2020)

        Returns:
            List[Tuple[str,int]]: list of (artist name, number of songs) tuples.
            If there are fewer than n artists, all of them are returned.
            If there are no artists, an empty list is returned.
        """
        s, e = yr
        self.cur.execute("""
                        SELECT a.name, COUNT(*)
                        FROM songs s
                        JOIN artists a ON s.artist_id = a.id
                        WHERE s.album_id IS NULL AND YEAR(s.release_date) BETWEEN %s AND %s
                        GROUP BY a.id
                        ORDER BY COUNT(*) DESC, a.name
                        LIMIT %s
                        """, (s, e, n))

        return self.cur.fetchall()

    def get_artists_last_single_in_year(self, year: int) -> Set[str]:
        """
        Get all artists who released their last single in the given year.
        
        Args:
            mydb: database connection
            year: year of last release
            
        Returns:
            Set[str]: set of artist names
            If there is no artist with a single released in the given year, an empty set is returned.
        """
        self.cur.execute("""
                        SELECT a.name
                        FROM artists a
                        JOIN songs s ON a.id = s.artist_id
                        WHERE s.album_id IS NULL
                        GROUP BY a.id
                        HAVING YEAR(MAX(s.release_date)) = %s
                        """, (year,))
        return {r[0] for r in self.cur.fetchall()}

    def get_top_song_genres(self, n: int) -> List[Tuple[str, int]]:
        """
        Get n genres that are most represented in terms of number of songs in that genre.
        Songs include singles as well as songs in albums. 
        
        Args:
            mydb: database connection
            n: number of genres

        Returns:
            List[Tuple[str,int]]: list of tuples (genre,number_of_songs), from most represented to
            least represented genre. If number of genres is less than n, returns all.
            Ties broken by alphabetical order of genre names.
        """
        self.cur.execute(
            "SELECT g.name, COUNT(*) FROM song_genres sg JOIN genres g ON sg.genre_id = g.id "
            "GROUP BY g.id ORDER BY COUNT(*) DESC, g.name LIMIT %s", (n,))
        return self.cur.fetchall()

    def get_album_and_single_artists(self) -> Set[str]:
        """
        Get artists who have released albums as well as singles.

        Args:
            mydb; database connection

        Returns:
            Set[str]: set of artist names
        """
        self.cur.execute("""SELECT a.name FROM artists a WHERE EXISTS
                           (SELECT 1 FROM songs WHERE artist_id = a.id AND album_id IS NOT NULL)
                           AND EXISTS (SELECT 1 FROM songs WHERE artist_id = a.id AND album_id IS NULL)""")
        return {r[0] for r in self.cur.fetchall()}

    def get_most_rated_songs(self, yr: Tuple[int, int], n: int) -> List[Tuple[str, str, int]]:
        """
        Get the top n most rated songs in the given year range (both inclusive), 
        ranked from most rated to least rated. 
        "Most rated" refers to number of ratings, not actual rating scores. 
        Ties are broken in alphabetical order of song title. If the number of rated songs is less
        than n, all rates songs are returned.
        
        Args:
            mydb: database connection
            year_range: range of years, e.g. (2018-2021), during which ratings were given
            n: number of most rated songs

        Returns:
            List[Tuple[str,str,int]: list of (song title, artist name, number of ratings for song)   
        """
        s, e = yr
        self.cur.execute("""SELECT s.title, a.name, COUNT(*) FROM ratings r
                        JOIN songs s ON r.song_id = s.id
                        JOIN artists a ON s.artist_id = a.id
                        WHERE YEAR(r.rating_date) BETWEEN %s AND %s
                        GROUP BY s.id
                        ORDER BY COUNT(*) DESC, s.title
                        LIMIT %s""", (s, e, n))
        return self.cur.fetchall()

    def get_most_engaged_users(self, yr: Tuple[int, int], n: int) -> List[Tuple[str, int]]:
        """
        Get the top n most engaged users, in terms of number of songs they have rated.
        Break ties by alphabetical order of usernames.

        Args:
            mydb: database connection
            year_range: range of years, e.g. (2018-2021), during which ratings were given
            n: number of users

        Returns:
            List[Tuple[str, int]]: list of (username,number_of_songs_rated) tuples
        """
        s, e = yr
        self.cur.execute("""SELECT u.username, COUNT(*) FROM ratings r JOIN users u ON r.user_id = u.id
                           WHERE YEAR(r.rating_date) BETWEEN %s AND %s
                           GROUP BY u.id ORDER BY COUNT(*) DESC, u.username LIMIT %s""", (s, e, n))
        return self.cur.fetchall()

def main():
    pass

if __name__ == "__main__":
    main()