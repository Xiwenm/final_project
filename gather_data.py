import requests
import json
import sqlite3
import re
import bs4

GOOGLE_BOOKS_URL = "https://www.googleapis.com/books/v1/volumes"
OMDB_URL = "https://www.omdbapi.com/"
OMDB_API_KEY = ""
DB_NAME = "final_project.db"


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

def scrape_adaptations_if_needed(conn, min_count=100):
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
    Return up to `limit` titles from Adaptations that do NOT yet appear
    in either Books OR Movies.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT A.title
        FROM Adaptations AS A
        LEFT JOIN Books  AS B ON A.title = B.book_title
        LEFT JOIN Movies AS M ON A.title = M.movie_title
        WHERE B.book_title IS NULL
          AND M.movie_title IS NULL
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