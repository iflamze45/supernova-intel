"""
SCOUT Agent — Claude API-powered football prediction intelligence.
MiroFish pattern: Seed → Analyze → Conflict Detection → Verdict
"""
import os
import json
import re
from typing import Optional
from pathlib import Path

from dotenv import load_dotenv
import anthropic

load_dotenv(Path(__file__).parents[2] / ".env")

from .models import SessionBrief, Prediction, ConfidenceTier

SCOUT_SYSTEM_PROMPT = """You are SCOUT — Statistical Computation & Odds Understanding Terminal.

You are a football prediction intelligence agent with a dual operating mode:
- ICE MODE — Calm, data-driven, structured. Probabilities, patterns, evidence. No hype.
- FLAMZE MODE — Bold, direct, decisive. Call the pick with conviction.

You use the MiroFish reasoning pattern: Seed → Analyze → Conflict Detection → Verdict.

BROWSING HIERARCHY — DATA SOURCES (follow this order):
1. Oddsportal (https://www.oddsportal.com) — live odds from 50+ bookmakers, odds movement
2. Flashscore (https://www.flashscore.com) — recent form (last 5-10), H2H, home/away record
3. SofaScore (https://www.sofascore.com) — deep stats, xG, player ratings, possession
4. Livescore (https://www.livescore.com) — lineup confirmation, injury updates

3-LAYER ANALYSIS:
Layer 1 — Evidence Gathering: Extract odds, form, H2H, xG, injuries from all four sources.
Layer 2 — Conflict Detection: Does form agree with odds? Overpriced/underpriced teams? Sharp money movement? xG vs raw results tension?
Layer 3 — Verdict Synthesis: Assign probabilities (must sum to 100%), confidence tier, recommended pick.

CONFIDENCE TIERS:
- LOCK 🔵 — All signals align
- LEAN 🟢 — Majority agree, minor conflicts
- SPLIT 🟡 — Signals genuinely divided
- AVOID 🔴 — Too much noise, no clear edge

HARD RULES:
1. Never fabricate data. If a source fails, say so explicitly.
2. Never guarantee outcomes — deal in probabilities and edges.
3. Always complete all four browsing layers before verdict.
4. AVOID is a valid and valuable output.
5. Odds displayed as decimal.

OUTPUT: You must respond with a JSON object in this exact structure:
{
  "match": "Home Team vs Away Team",
  "competition": "Competition Name",
  "date": "Date | Kickoff Time",
  "home_win_pct": 45,
  "draw_pct": 28,
  "away_win_pct": 27,
  "confidence": "LOCK|LEAN|SPLIT|AVOID",
  "recommended_pick": "Home Win",
  "recommended_odds": "1.85",
  "sources_checked": ["Oddsportal", "Flashscore", "SofaScore", "Livescore"],
  "flamze_breakdown": "Full analyst narrative here...",
  "sources_unavailable": []
}

After the JSON, you may add any extended analysis."""


class ScoutAgent:
    def __init__(self, api_key: Optional[str] = None):
        self.client = anthropic.Anthropic(
            api_key=api_key or os.environ.get("ANTHROPIC_API_KEY")
        )
        self.session_predictions: list[Prediction] = []

    def analyze(self, brief: SessionBrief) -> Prediction:
        """Run the full MiroFish analysis loop for a match."""
        user_message = self._build_user_message(brief)

        response = self.client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=SCOUT_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}]
        )

        raw = response.content[0].text
        prediction = self._parse_response(raw, brief)
        self.session_predictions.append(prediction)
        return prediction

    def _build_user_message(self, brief: SessionBrief) -> str:
        session_context = ""
        if self.session_predictions:
            prior = [p.match for p in self.session_predictions]
            session_context = f"\n\nPRIOR MATCHES THIS SESSION: {', '.join(prior)}"

        return f"""SCOUT SESSION BRIEF

Match: {brief.match}
Market Focus: {brief.market_focus}
Context: {brief.context or 'None provided'}
{session_context}

Execute the full 3-layer MiroFish analysis. Browse all four data sources in hierarchy order.
Deliver the JSON prediction block followed by your FLAMZE breakdown."""

    def _parse_response(self, raw: str, brief: SessionBrief) -> Prediction:
        """Extract structured prediction from Claude response."""
        json_match = re.search(r'\{[\s\S]*?"flamze_breakdown"[\s\S]*?\}', raw)

        if json_match:
            try:
                data = json.loads(json_match.group())
                return Prediction(
                    match=data.get("match", brief.match),
                    competition=data.get("competition", "Unknown"),
                    date=data.get("date", "TBD"),
                    home_win_pct=float(data.get("home_win_pct", 33)),
                    draw_pct=float(data.get("draw_pct", 33)),
                    away_win_pct=float(data.get("away_win_pct", 34)),
                    confidence=ConfidenceTier(data.get("confidence", "SPLIT")),
                    recommended_pick=data.get("recommended_pick", "No pick"),
                    recommended_odds=data.get("recommended_odds", "N/A"),
                    sources_checked=data.get("sources_checked", []),
                    flamze_breakdown=data.get("flamze_breakdown", ""),
                    ice_block="",
                    raw_response=raw,
                )
            except (json.JSONDecodeError, ValueError):
                pass

        # Fallback — return raw response wrapped in a minimal Prediction
        return Prediction(
            match=brief.match,
            competition="Unknown",
            date="TBD",
            home_win_pct=0,
            draw_pct=0,
            away_win_pct=0,
            confidence=ConfidenceTier.SPLIT,
            recommended_pick="See breakdown",
            recommended_odds="N/A",
            sources_checked=[],
            flamze_breakdown=raw,
            ice_block="",
            raw_response=raw,
        )
