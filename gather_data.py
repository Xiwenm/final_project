import requests
import json
import sqlite3
import re
import bs4

GOOGLE_BOOKS_URL = "https://www.googleapis.com/books/v1/volumes"
OMDB_URL = "https://www.omdbapi.com/"
OMDB_API_KEY = "d4a57588" # Change here
DB_NAME = "final_project.db"
maximumcount = 300 


def create_connection():
    return sqlite3.connect(DB_NAME)


def create_tables(conn):
    """
    Create all tables needed for the project.

    Titles: scraped list of book titles that have film adaptations
    Books: Google Books data, linked to Titles via title_id
    Movies: OMDb data, linked to Titles via title_id
    FailedTitles: titles that failed either API
    """
    cur = conn.cursor()

    # Scraped titles
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Titles (
            title_id INTEGER PRIMARY KEY,
            title    TEXT UNIQUE
        )
    """)

    # Google Books
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Books (
            book_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            title_id      INTEGER UNIQUE,
            book_rating   REAL,
            ratings_count INTEGER,
            FOREIGN KEY (title_id) REFERENCES Titles(title_id)
        )
    """)

    # OMDb
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Movies (
            movie_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            title_id    INTEGER UNIQUE,
            movie_rating REAL,
            movie_count  INTEGER,
            FOREIGN KEY (title_id) REFERENCES Titles(title_id)
        )
    """)

    # Failed titles
    cur.execute("""
        CREATE TABLE IF NOT EXISTS FailedTitles (
        failed_id INTEGER PRIMARY KEY AUTOINCREMENT,
        title_id  INTEGER UNIQUE,
        FOREIGN KEY (title_id) REFERENCES Titles(title_id)
        )
    """)

    conn.commit()


def clean_goodreads_title(raw_title: str) -> str:
    """
    Clean Goodreads list titles like:
      'The Hunger Games (The Hunger Games, #1)'
    into:
      'The Hunger Games'
    """
    match = re.match(r"^(.*?)(\s*\(.*?#\d+.*\))$", raw_title)
    if match:
        return match.group(1).strip()
    return raw_title.strip()


def scrape_titles_if_needed(conn, max_count=maximumcount):
    """
    Scrape Goodreads list:
    https://www.goodreads.com/list/show/87198.Books_Made_into_Movies_or_TV_Shows

    Each run:
      - Only runs if Titles has fewer than min_count rows.
      - Scrapes pages one by one.
      - Inserts at most 25 NEW titles into Titles, then stops.
    """
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM Titles")
    row = cur.fetchone()
    current_count = row[0] if row else 0

    if current_count >= max_count:
        print(f"Titles already has {current_count} rows; skipping Goodreads scrape.")
        return

    print("Scraping Goodreads list: Books Made into Movies or TV Shows ...")
    base_url = "https://www.goodreads.com/list/show/87198.Books_Made_into_Movies_or_TV_Shows"

    max_pages = 5          
    max_new_per_run = 25 

    inserted = 0

    for page_num in range(1, max_pages + 1):
        if inserted >= max_new_per_run or (current_count + inserted) >= max_count:
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

        # book title is an <a> whose href looks like /book/show/...
        all_links = soup.find_all("a", href=re.compile(r"/book/show/"))

        for link in all_links:
            if inserted >= max_new_per_run or (current_count + inserted) >= max_count:
                break  

            raw_title = link.get_text(strip=True)
            if not raw_title:
                continue

            cleaned = clean_goodreads_title(raw_title)
            if not cleaned:
                continue

            try:
                cur.execute(
                    "INSERT OR IGNORE INTO Titles (title) VALUES (?)",
                    (cleaned,)
                )

                if cur.rowcount > 0:
                    inserted += 1
            except sqlite3.Error:
                continue

    conn.commit()
    print(f"Inserted {inserted} new titles into Titles this run.")


def get_pending_titles(conn, limit):
    """
    Return up to `limit` titles from Titles that:
      - do NOT yet have a row in Books
      - do NOT yet have a row in Movies
      - are NOT marked as failed in FailedTitles

    Returns a list of (title_id, title) tuples.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT T.title_id, T.title
        FROM Titles AS T
        LEFT JOIN Books        AS B ON T.title_id = B.title_id
        LEFT JOIN Movies       AS M ON T.title_id = M.title_id
        LEFT JOIN FailedTitles AS F ON T.title_id = F.title_id
        WHERE B.title_id IS NULL
          AND M.title_id IS NULL
          AND F.title_id IS NULL
        LIMIT ?
    """, (limit,))

    return cur.fetchall()


def fetch_google_books_raw(title):
    """
    Call Google Books API and return the raw JSON.
    """
    params = {"q": title, "maxResults": 1}
    try:
        resp = requests.get(GOOGLE_BOOKS_URL, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except (requests.RequestException, json.JSONDecodeError) as e:
        print(f"Google Books request failed for '{title}': {e}")
        return None


def parse_google_books_entry(raw_json):
    """
    Extract [book_rating, ratings_count] from Google Books JSON.
    (We keep the title string in Titles, not here.)
    """
    if not raw_json:
        return None

    items = raw_json.get("items")
    if not items:
        return None

    volume_info = items[0].get("volumeInfo", {})
    title = volume_info.get("title")
    if not title:
        return None

    return {
        "book_rating":   volume_info.get("averageRating"),
        "ratings_count": volume_info.get("ratingsCount")
    }


def fetch_omdb_raw(title):
    """
    Call OMDb API and return raw JSON.
    """
    if not OMDB_API_KEY:
        print("Set OMDB_API_KEY in gather_data.py before running OMDb requests.")
        return None

    params = {"t": title, "apikey": OMDB_API_KEY}
    try:
        resp = requests.get(OMDB_URL, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except (requests.RequestException, json.JSONDecodeError) as e:
        print(f"OMDb request failed for '{title}': {e}")
        return None


def parse_omdb_entry(raw_json):
    """
    Extract [movie_rating, movie_count] from OMDb JSON.
    """
    if not raw_json or raw_json.get("Response") != "True":
        return None

    imdb_rating_val = None
    rating_str = raw_json.get("imdbRating")
    if rating_str and rating_str != "N/A":
        try:
            imdb_rating_val = float(rating_str)
        except ValueError:
            imdb_rating_val = None

    imdb_votes_val = None
    votes_str = raw_json.get("imdbVotes")
    if votes_str and votes_str != "N/A":
        try:
            imdb_votes_val = int(votes_str.replace(",", ""))
        except ValueError:
            imdb_votes_val = None

    return {
        "movie_rating": imdb_rating_val,
        "movie_count":  imdb_votes_val
    }


def insert_adaptation(conn, title_id, book_data, movie_data):
    """
    Insert one book+movie pair into:
    - Books (linked by title_id)
    - Movies (linked by title_id)
    """
    cur = conn.cursor()


    cur.execute("""
        INSERT OR IGNORE INTO Books (title_id, book_rating, ratings_count)
        VALUES (?, ?, ?)
    """, (
        title_id,
        book_data.get("book_rating"),
        book_data.get("ratings_count")
    ))


    cur.execute("""
        INSERT OR IGNORE INTO Movies (title_id, movie_rating, movie_count)
        VALUES (?, ?, ?)
    """, (
        title_id,
        movie_data.get("movie_rating"),
        movie_data.get("movie_count")
    ))

    conn.commit()


def load_batch(conn, max_new=25):
    """
    Load up to max_new NEW adaptations in one run.
    This enforces the 25-items-per-run rule from the project spec.
    """
    candidate_rows = get_pending_titles(conn, max_new)

    inserted = 0
    for title_id, title in candidate_rows:
        if inserted >= max_new:
            break

        print(f"Processing '{title}' (title_id={title_id}) ...")

        # Google Books
        gb_raw = fetch_google_books_raw(title)
        gb_data = parse_google_books_entry(gb_raw)
        if gb_data is None:
            print("  Skipping: no valid Google Books data.")
            cur = conn.cursor()
            cur.execute(
                "INSERT OR IGNORE INTO FailedTitles (title_id) VALUES (?)",
                (title_id,)
            )

            conn.commit()
            continue

        # OMDb
        omdb_raw = fetch_omdb_raw(title)
        omdb_data = parse_omdb_entry(omdb_raw)
        if omdb_data is None:
            print("  Skipping: no valid OMDb data.")
            cur = conn.cursor()
            cur.execute(
                "INSERT OR IGNORE INTO FailedTitles (title_id) VALUES (?)",
                (title_id,)
            )

            conn.commit()
            continue

        insert_adaptation(conn, title_id, gb_data, omdb_data)
        inserted += 1
        print(f"  Inserted adaptation #{inserted} this run.")

    print(f"Finished batch: {inserted} new adaptations inserted (max {max_new}).")


def main():
    conn = create_connection()
    create_tables(conn)

    scrape_titles_if_needed(conn, max_count=maximumcount)
    load_batch(conn, max_new=25)

    conn.close()


if __name__ == "__main__":
    main()
