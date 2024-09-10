from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import duckdb

app = FastAPI()

# Initialize DuckDB connection
conn = duckdb.connect('books.db')

# Create a table
conn.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        name VARCHAR(100),
        email VARCHAR(100)
    )
''')


class User(BaseModel):
    name: str
    email: str


class UserInDB(User):
    id: int


@app.post("/users/", response_model=UserInDB)
def create_user(user: User):
    # Insert user into the database
    result = conn.execute(
        "INSERT INTO users (name, email) VALUES (?, ?) RETURNING id",
        (user.name, user.email)
    ).fetchone()

    if result:
        user_id = result[0]
        return UserInDB(id=user_id, **user.dict())
    else:
        raise HTTPException(status_code=500, detail="Failed to create user")


@app.get("/users/{user_id}", response_model=UserInDB)
def read_user(user_id: int):
    result = conn.execute("SELECT * FROM users WHERE id = ?", [user_id]).fetchone()
    if result:
        return UserInDB(id=result[0], name=result[1], email=result[2])
    else:
        raise HTTPException(status_code=404, detail="User not found")


@app.get("/users/", response_model=list[UserInDB])
def read_users():
    results = conn.execute("SELECT * FROM users").fetchall()
    return [UserInDB(id=row[0], name=row[1], email=row[2]) for row in results]


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)