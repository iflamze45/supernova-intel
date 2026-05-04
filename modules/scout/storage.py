"""Persist SCOUT predictions into supernova.db."""
import sqlite3
import json
import hashlib
from datetime import datetime
from pathlib import Path

from .models import Prediction

DB_PATH = Path(__file__).parents[2] / "knowledge-graph" / "supernova.db"


def _ensure_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scout_predictions (
            id TEXT PRIMARY KEY,
            match TEXT NOT NULL,
            competition TEXT,
            date TEXT,
            home_win_pct REAL,
            draw_pct REAL,
            away_win_pct REAL,
            confidence TEXT,
            recommended_pick TEXT,
            recommended_odds TEXT,
            flamze_breakdown TEXT,
            sources_checked TEXT,
            created_at TEXT
        )
    """)
    conn.commit()


def save_prediction(prediction: Prediction, db_path: str = None):
    path = db_path or str(DB_PATH)
    conn = sqlite3.connect(path)
    _ensure_table(conn)

    pred_id = hashlib.md5(
        f"{prediction.match}:{prediction.created_at}".encode()
    ).hexdigest()

    conn.execute("""
        INSERT OR REPLACE INTO scout_predictions
        (id, match, competition, date, home_win_pct, draw_pct, away_win_pct,
         confidence, recommended_pick, recommended_odds, flamze_breakdown,
         sources_checked, created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        pred_id,
        prediction.match,
        prediction.competition,
        prediction.date,
        prediction.home_win_pct,
        prediction.draw_pct,
        prediction.away_win_pct,
        prediction.confidence.value,
        prediction.recommended_pick,
        prediction.recommended_odds,
        prediction.flamze_breakdown,
        json.dumps(prediction.sources_checked),
        prediction.created_at,
    ))
    conn.commit()
    conn.close()
    return pred_id


def get_recent_predictions(limit: int = 10, db_path: str = None) -> list[dict]:
    path = db_path or str(DB_PATH)
    conn = sqlite3.connect(path)
    _ensure_table(conn)
    cursor = conn.execute("""
        SELECT match, competition, confidence, recommended_pick, recommended_odds, created_at
        FROM scout_predictions
        ORDER BY created_at DESC
        LIMIT ?
    """, (limit,))
    rows = [
        {
            "match": r[0], "competition": r[1], "confidence": r[2],
            "pick": r[3], "odds": r[4], "date": r[5]
        }
        for r in cursor.fetchall()
    ]
    conn.close()
    return rows
