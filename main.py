# main.py
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
import sqlite3
import uuid
import time
import threading

app = FastAPI(
    title="Unix-Inspired Task Manager API",
    description="A simple task manager inspired by Unix commands"
)

# Database setup
def init_db():
    conn = sqlite3.connect('tasks.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY,
            name TEXT,
            status TEXT,
            created_at TEXT,
            completed_at TEXT
        )
    ''')
    conn.commit()
    conn.close()

init_db()

# Models
class Task(BaseModel):
    id: str
    name: Optional[str] = None
    status: str
    created_at: str
    completed_at: Optional[str] = None

class TaskCreate(BaseModel):
    name: Optional[str] = None

class TaskUpdate(BaseModel):
    status: Optional[str] = None
    name: Optional[str] = None

# Helper functions
def get_db_connection():
    conn = sqlite3.connect('tasks.db')
    conn.row_factory = sqlite3.Row
    return conn

def row_to_task(row):
    return Task(
        id=row['id'],
        name=row['name'],
        status=row['status'],
        created_at=row['created_at'],
        completed_at=row['completed_at']
    )

# API Endpoints
@app.post("/tasks", response_model=Task, status_code=status.HTTP_201_CREATED)
def create_task(task_data: TaskCreate):
    """Create a new task that completes after 10 seconds"""
    task_id = str(uuid.uuid4())
    created_at = datetime.now().isoformat()
    
    # Initial task object with "running" status
    task = {
        "id": task_id,
        "name": task_data.name,
        "status": "running",
        "created_at": created_at,
        "completed_at": None
    }
    
    # Save to database immediately
    conn = get_db_connection()
    conn.execute(
        "INSERT INTO tasks (id, name, status, created_at, completed_at) VALUES (?, ?, ?, ?, ?)",
        (task['id'], task['name'], task['status'], task['created_at'], task['completed_at'])
    )
    conn.commit()
    conn.close()
    
    # Start background thread to complete the task after 20 seconds
    def complete_task():
        time.sleep(20)  # Wait 20 seconds
        completed_at = datetime.now().isoformat()
        
        conn = get_db_connection()
        conn.execute(
            "UPDATE tasks SET status = ?, completed_at = ? WHERE id = ?",
            ("completed", completed_at, task_id)
        )
        conn.commit()
        conn.close()
    
    threading.Thread(target=complete_task).start()
    
    return task

@app.get("/tasks", response_model=List[Task])
def list_tasks(status: Optional[str] = None, limit: Optional[int] = None):
    """List all tasks (like ls)"""
    conn = get_db_connection()
    query = "SELECT * FROM tasks"
    params = []
    
    if status:
        query += " WHERE status = ?"
        params.append(status)
    
    query += " ORDER BY created_at DESC"
    
    if limit:
        query += " LIMIT ?"
        params.append(limit)
    
    rows = conn.execute(query, params).fetchall()
    conn.close()
    
    return [row_to_task(row) for row in rows]

@app.get("/tasks/{task_id}", response_model=Task)
def get_task(task_id: str):
    """Get details of a specific task"""
    conn = get_db_connection()
    task = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
    conn.close()
    
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return row_to_task(task)

@app.patch("/tasks/{task_id}", response_model=Task)
def update_task(task_id: str, task_data: TaskUpdate):
    """Update task status or name"""
    conn = get_db_connection()
    task = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
    
    if task is None:
        conn.close()
        raise HTTPException(status_code=404, detail="Task not found")
    
    updates = {}
    if task_data.status:
        updates['status'] = task_data.status
        if task_data.status == "completed":
            updates['completed_at'] = datetime.now().isoformat()
    if task_data.name:
        updates['name'] = task_data.name
    
    if updates:
        set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
        values = list(updates.values())
        values.append(task_id)
        
        conn.execute(
            f"UPDATE tasks SET {set_clause} WHERE id = ?",
            values
        )
        conn.commit()
    
    updated_task = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
    conn.close()
    
    return row_to_task(updated_task)

@app.delete("/tasks/{task_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_task(task_id: str):
    """Delete a task"""
    conn = get_db_connection()
    result = conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
    conn.commit()
    conn.close()
    
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Task not found")