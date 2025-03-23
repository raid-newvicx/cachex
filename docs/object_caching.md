# Object Caching
There are certain scenarios where you may want to cache an object such as a database connection, HTTP client or ML model. `cache_reference` is an easy way to cache unhashable objects and make them available globally throughout an application. It is a very easy way to implement the singleton design pattern and initialize application state. Both sync and async functions are supported.

[Lifespan Events](https://fastapi.tiangolo.com/advanced/events/#use-case) are a common way to initialize application state on start and clean up resources on close. In **most** cases, this is the correct approach. `cache_reference` is not a replacement for lifespan events. It is useful in certain situations, for example...

- **Lazy Initialization**: Creating objects when needed on demand ensures resources are not unecessarily allocated. If the initialization of an object is not prohibitively slow, and is only used for a subset of endpoints, it may make sense to initialize that object when needed rather than on application start.
- **Dynamic Configuration**: The configuration of an object may vary based on user input (or the user themselves). It may not be feasible to initialize all objects at startup. The example below illustrates a per-user database configuration using `sqlite3`...

```python
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
```