import sqlite3
from typing import Annotated

from cachex import cache_reference
from fastapi import Depends, FastAPI


app = FastAPI()


@cache_reference()
def get_conn(user: str) -> sqlite3.Connection:
    """Create a DB file for the user."""
    conn = sqlite3.connect(f"{user}.db")
    # Just for the sake of the example we will create a table and populate some data.
    # It will be the same data for all users
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS movie(title, year, score)")
    cur.execute("""
        INSERT INTO movie VALUES
            ('Monty Python and the Holy Grail', 1975, 8.2),
            ('And Now for Something Completely Different', 1971, 7.5)
    """)
    return conn


# This is an intentionally simple and insecure example intended to demonstrate how to use
# the `cache_reference` API. In a real application, users should be authenticated and authorized
# to access the data
@app.get("/name/{user}")
def get_movie_name(conn: Annotated[sqlite3.Connection, Depends(get_conn)]) -> str:
    """Query data from the user's database."""
    cur = conn.cursor()
    result = cur.execute("SELECT title FROM movie")
    return result.fetchone()[0]