import os
import psycopg2
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI(title="BMW Vehicle Software Registry")
Instrumentator().instrument(app).expose(app)

DB_URL = os.getenv("DATABASE_URL")

def get_conn():
    return psycopg2.connect(DB_URL)

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vehicles (
            vin TEXT PRIMARY KEY,
            model TEXT,
            current_version TEXT,
            update_status TEXT DEFAULT 'up-to-date',
            last_updated TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS releases (
            id SERIAL PRIMARY KEY,
            version TEXT NOT NULL,
            environment TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            deployed_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    conn.commit()
    conn.close()

init_db()

class Vehicle(BaseModel):
    vin: str
    model: str
    current_version: str

class Release(BaseModel):
    version: str
    environment: str

@app.get("/")
def root():
    return {"service": "BMW Vehicle Software Registry", "status": "running"}

@app.post("/vehicles")
def register_vehicle(vehicle: Vehicle):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO vehicles (vin, model, current_version)
        VALUES (%s, %s, %s) ON CONFLICT (vin) DO NOTHING
    """, (vehicle.vin, vehicle.model, vehicle.current_version))
    conn.commit()
    conn.close()
    return {"message": f"Vehicle {vehicle.vin} registered successfully"}

@app.get("/vehicles")
def list_vehicles():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM vehicles ORDER BY last_updated DESC")
    rows = cur.fetchall()
    conn.close()
    return [{"vin": r[0], "model": r[1], "current_version": r[2], "update_status": r[3], "last_updated": str(r[4])} for r in rows]

@app.post("/releases")
def create_release(release: Release):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO releases (version, environment, status)
        VALUES (%s, %s, 'deployed')
    """, (release.version, release.environment))
    if release.environment == "production":
        cur.execute("""
            UPDATE vehicles SET current_version = %s,
            update_status = 'updated', last_updated = NOW()
        """, (release.version,))
    conn.commit()
    conn.close()
    return {"message": f"Version {release.version} deployed to {release.environment}"}

@app.get("/releases")
def list_releases():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM releases ORDER BY deployed_at DESC")
    rows = cur.fetchall()
    conn.close()
    return [{"id": r[0], "version": r[1], "environment": r[2], "status": r[3], "deployed_at": str(r[4])} for r in rows]

@app.get("/health")
def health():
    return {"status": "healthy", "timestamp": str(datetime.utcnow())}