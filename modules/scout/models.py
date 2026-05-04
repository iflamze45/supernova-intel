from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
from typing import Optional


class ConfidenceTier(str, Enum):
    LOCK = "LOCK"
    LEAN = "LEAN"
    SPLIT = "SPLIT"
    AVOID = "AVOID"

    @property
    def symbol(self):
        return {"LOCK": "🔵", "LEAN": "🟢", "SPLIT": "🟡", "AVOID": "🔴"}[self.value]


@dataclass
class SessionBrief:
    match: str           # "Home Team vs Away Team | Competition | Date/Kickoff"
    market_focus: str    # "W/D/L only" or "W/D/L + secondary"
    context: str = ""    # injuries, suspensions, must-win, etc.


@dataclass
class Prediction:
    match: str
    competition: str
    date: str
    home_win_pct: float
    draw_pct: float
    away_win_pct: float
    confidence: ConfidenceTier
    recommended_pick: str
    recommended_odds: str
    ice_block: str        # structured ICE MODE output
    flamze_breakdown: str # analyst FLAMZE MODE narrative
    sources_checked: list = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    raw_response: str = ""

    def ice_display(self) -> str:
        sources = "  ".join([f"✓ {s}" for s in self.sources_checked])
        return f"""
{'━' * 50}
SCOUT PREDICTION | {self.match}
{self.competition} | {self.date}
{'━' * 50}

PROBABILITY ESTIMATE
  Home Win .......... {self.home_win_pct:.0f}%
  Draw .............. {self.draw_pct:.0f}%
  Away Win .......... {self.away_win_pct:.0f}%

CONFIDENCE TIER
  {self.confidence.symbol} {self.confidence.value}

RECOMMENDED PICK
  {self.recommended_pick} @ {self.recommended_odds}

DATA SOURCES CHECKED
  {sources}
{'━' * 50}"""
