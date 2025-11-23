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