import requests
import json
import sqlite3
import re
import bs4

GOOGLE_BOOKS_URL = "https://www.googleapis.com/books/v1/volumes"
OMDB_URL = "https://www.omdbapi.com/"
OMDB_API_KEY = "" # Enter API Here
DB_NAME = "final_project.db"
minimumcount = 300 # Amount of names we want from the Goodreads


def create_connection():
    conn = sqlite3.connect(DB_NAME)
    return conn

def create_tables(conn):
    """
    Create all tables needed for the project.
    
    Adaptations: scraped list of book titles that have film adaptations
    Books: Google Books data
    Movies: OMDb data
    Book_Movie: join table (integer keys book_id + movie_id)
    """
    cur = conn.cursor()

    # Scraped titles
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Adaptations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT UNIQUE,
            source_url TEXT
        )
    """)

    # Google Books
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Books (
            book_id INTEGER PRIMARY KEY AUTOINCREMENT,
            book_title TEXT UNIQUE,
            authors TEXT,
            book_rating REAL,
            ratings_count INTEGER
        )
    """)

    # OMDb
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Movies (
            movie_id INTEGER PRIMARY KEY AUTOINCREMENT,
            movie_title TEXT UNIQUE,
            movie_rating REAL,
            movie_count INTEGER
        )
    """)

    # Join
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Book_Movie (
            matching_id INTEGER PRIMARY KEY AUTOINCREMENT,
            book_id INTEGER,
            movie_id INTEGER,
            FOREIGN KEY(book_id) REFERENCES Books(book_id),
            FOREIGN KEY(movie_id) REFERENCES Movies(movie_id)
        )
    """)

    #Failed Titles
    cur.execute("""
        CREATE TABLE IF NOT EXISTS FailedTitles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT UNIQUE
        )
    """)

    conn.commit()

def clean_goodreads_title(raw_title):
    """
    Clean Goodreads list titles like:
      'The Hunger Games (The Hunger Games, #1)'
    into:
      'The Hunger Games'

    Strategy:
    - If there is a trailing parenthesis that contains '#', strip it.
    - Strip extra whitespace.
    """
    title = raw_title

    match = re.match(r"^(.*?)(\s*\(.*?#\d+.*\))$", title)
    if match:
        title = match.group(1)
    return title.strip()

def scrape_adaptations_if_needed(conn, min_count=minimumcount):
    """
    Scrape Goodreads list:
    https://www.goodreads.com/list/show/87198.Books_Made_into_Movies_or_TV_Shows

    Collect at least min_count BOOK TITLES (books that have movies/TV adaptations).
    Runs ONLY if Adaptations has fewer than min_count rows.
    """
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM Adaptations")
    row = cur.fetchone()
    if row is None:
        current_count = 0
    else:
        current_count = row[0]

    if current_count >= min_count:
        print(f"Adaptations already has {current_count} rows; skip Goodreads scrape.")
        return

    print("Scraping Goodreads list: Books Made into Movies or TV Shows ...")

    base_url = "https://www.goodreads.com/list/show/87198.Books_Made_into_Movies_or_TV_Shows"

    scraped_titles = []

    # Goodreads list has multiple pages, loop through a few pages until we have enough
    max_pages = 5
    for page_num in range(1, max_pages + 1):
        if current_count + len(scraped_titles) >= min_count:
            break

        page_url = f"{base_url}?page={page_num}"
        print(f"  Fetching page {page_num}: {page_url}")

        try:
            response = requests.get(
                page_url,
                timeout=30,
                headers={"User-Agent": "Mozilla/5.0"}
            )
            response.raise_for_status()
        except requests.RequestException as e:
            print("  Goodreads request failed on page", page_num, ":", e)
            break

        soup = bs4.BeautifulSoup(response.text, "html.parser")

        #Book title is an <a> whose href looks like /book/show/...
        all_links = soup.find_all("a", href=re.compile(r"/book/show/"))

        for link in all_links:
            raw_title = link.get_text(strip=True)
            if not raw_title:
                continue

            cleaned_title = clean_goodreads_title(raw_title)
            if not cleaned_title:
                continue

            scraped_titles.append(cleaned_title)

    print(f"Found {len(scraped_titles)} raw book title links on Goodreads.")

    inserted = 0
    for title in scraped_titles:
        if current_count + inserted >= min_count:
            break

        try:
            cur.execute(
                "INSERT OR IGNORE INTO Adaptations (title, source_url) VALUES (?, ?)",
                (title, base_url)
            )
            if cur.rowcount > 0:
                inserted = inserted + 1
        except sqlite3.Error:
            continue

    conn.commit()
    print(f"Inserted {inserted} Goodreads book titles into Adaptations.")


def get_pending_titles(conn, limit):
    """
    Return up to `limit` titles from Adaptations that:
      - are NOT already in Books
      - are NOT already in Movies
      - are NOT marked as failed
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT A.title
        FROM Adaptations AS A
        LEFT JOIN Books        AS B ON A.title = B.book_title
        LEFT JOIN Movies       AS M ON A.title = M.movie_title
        LEFT JOIN FailedTitles AS F ON A.title = F.title
        WHERE B.book_title IS NULL
          AND M.movie_title IS NULL
          AND F.title IS NULL
        LIMIT ?
    """, (limit,))
    rows = cur.fetchall()

    titles = []
    for row in rows:
        titles.append(row[0])
    return titles


def fetch_google_books_raw(title):
    """
    Call Google Books API and return the raw JSON (as a Python dict via json.loads).
    """
    params = {
        "q": title,
        "maxResults": 1
    }
    try:
        response = requests.get(GOOGLE_BOOKS_URL, params=params, timeout=30)
        response.raise_for_status()
        data = json.loads(response.text)
        return data
    except (requests.RequestException, json.JSONDecodeError) as e:
        print(f"Google Books request failed for '{title}': {e}")
        return None

def parse_google_books_entry(raw_json):
    """
    Extract [book_title, authors, book_rating, ratings_count] from Google Books JSON.
    """
    if raw_json is None:
        return None

    items = raw_json.get("items")
    if not items:
        return None

    volume_info = items[0].get("volumeInfo", {})
    title = volume_info.get("title")
    if not title:
        return None

    authors_list = volume_info.get("authors")
    authors = None
    if isinstance(authors_list, list):
        authors = ", ".join(authors_list)

    avg_rating = volume_info.get("averageRating")
    ratings_count = volume_info.get("ratingsCount")

    result = {
        "book_title": title,
        "authors": authors,
        "book_rating": avg_rating,
        "ratings_count": ratings_count
    }
    return result


def fetch_omdb_raw(title):
    """
    Call OMDb API and return raw JSON (as Python dict via json.loads).
    """
    if OMDB_API_KEY == "YOUR_OMDB_API_KEY_HERE":
        print("Set OMDB_API_KEY in gather_data.py before running OMDb requests.")
        return None

    params = {
        "t": title,
        "apikey": OMDB_API_KEY
    }
    try:
        resp = requests.get(OMDB_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = json.loads(resp.text)
        return data
    except (requests.RequestException, json.JSONDecodeError) as e:
        print(f"OMDb request failed for '{title}': {e}")
        return None


def parse_omdb_entry(raw_json):
    """
    Extract [movie_title, movie_rating, movie_count] from OMDb JSON.
    """
    if raw_json is None:
        return None
    if raw_json.get("Response") != "True":
        return None

    title = raw_json.get("Title")

    imdb_rating_val = None
    imdb_rating_str = raw_json.get("imdbRating")
    if imdb_rating_str and imdb_rating_str != "N/A":
        try:
            imdb_rating_val = float(imdb_rating_str)
        except ValueError:
            imdb_rating_val = None

    imdb_votes_val = None
    votes_str = raw_json.get("imdbVotes")
    if votes_str and votes_str != "N/A":
        try:
            cleaned = votes_str.replace(",", "")
            imdb_votes_val = int(cleaned)
        except ValueError:
            imdb_votes_val = None

    result = {
        "movie_title": title,
        "movie_rating": imdb_rating_val,
        "movie_count": imdb_votes_val
    }
    return result

def insert_adaptation(conn, book_data, movie_data):
    """
    Insert one book+movie pair into:
    - Books
    - Movies
    - Book_Movie  (link table using integer keys)
    """
    cur = conn.cursor()

    # Insert or ignore the book
    cur.execute("""
        INSERT OR IGNORE INTO Books (book_title, authors, book_rating, ratings_count)
        VALUES (?, ?, ?, ?)
    """, (
        book_data.get("book_title"),
        book_data.get("authors"),
        book_data.get("book_rating"),
        book_data.get("ratings_count")
    ))

    # Get book_id
    cur.execute("SELECT book_id FROM Books WHERE book_title = ?", (book_data.get("book_title"),))
    row = cur.fetchone()
    if row is None:
        return
    book_id = row[0]

    # Insert or ignore the movie
    cur.execute("""
        INSERT OR IGNORE INTO Movies (movie_title, movie_rating, movie_count)
        VALUES (?, ?, ?)
    """, (
        movie_data.get("movie_title"),
        movie_data.get("movie_rating"),
        movie_data.get("movie_count")
    ))

    # Get movie_id
    cur.execute("SELECT movie_id FROM Movies WHERE movie_title = ?", (movie_data.get("movie_title"),))
    row = cur.fetchone()
    if row is None:
        return
    movie_id = row[0]

    # Insert into join table
    cur.execute("""
        INSERT OR IGNORE INTO Book_Movie (book_id, movie_id)
        VALUES (?, ?)
    """, (book_id, movie_id))

    conn.commit()


def load_batch(conn, max_new=25):
    """
    Load up to max_new NEW adaptations in one run.
    This enforces the 25-items-per-run rule from the project spec.
    """
    # Grab more than max_new candidate titles so if some fails
    candidate_titles = get_pending_titles(conn, max_new * 2)

    inserted = 0
    for scraped_title in candidate_titles:
        if inserted >= max_new:
            break

        print(f"Processing '{scraped_title}'...")

        # Google Books
        gb_raw = fetch_google_books_raw(scraped_title)
        gb_data = parse_google_books_entry(gb_raw)
        if gb_data is None:
            print("  Skipping: no valid Google Books data.")
            cur = conn.cursor()
            cur.execute("INSERT OR IGNORE INTO FailedTitles (title) VALUES (?)",
                        (scraped_title,))
            conn.commit()
            continue

        # Override title to keep consistent with scraped Goodreads title
        gb_data["book_title"] = scraped_title

        # OMDb
        omdb_raw = fetch_omdb_raw(scraped_title)
        omdb_data = parse_omdb_entry(omdb_raw)
        if omdb_data is None:
            print("  Skipping: no valid OMDb data.")
            cur = conn.cursor()
            cur.execute("INSERT OR IGNORE INTO FailedTitles (title) VALUES (?)",
                        (scraped_title,))
            conn.commit()
            continue


        insert_adaptation(conn, gb_data, omdb_data)
        inserted = inserted + 1
        print(f"  Inserted adaptation #{inserted} this run.")

    print(f"Finished batch: {inserted} new adaptations inserted (max {max_new}).")


def main():
    conn = create_connection()
    create_tables(conn)

    scrape_adaptations_if_needed(conn, min_count=minimumcount)

    load_batch(conn, max_new=25)

    conn.close()


if __name__ == "__main__":
    main()