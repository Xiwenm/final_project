import requests
import json
import sqlite3
import re

GOOGLE_BOOKS_URL = "https://www.googleapis.com/books/v1/volumes"
OMDB_URL = "https://www.omdbapi.com/"
OMDB_API_KEY = ""
DB_NAME = "final_project.db"


def create_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_NAME)
    return conn

def create_tables(conn: sqlite3.Connection):
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