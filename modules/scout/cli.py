#!/usr/bin/env python3
"""
SCOUT CLI — Football Prediction Intelligence Terminal
Usage:
  python -m modules.scout.cli
  python -m modules.scout.cli --match "Arsenal vs Chelsea | Premier League | 25 Apr 2026 | 20:00"
  python -m modules.scout.cli --history
"""
import argparse
import sys
import os
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).parents[2] / ".env")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from modules.scout.scout_agent import ScoutAgent
from modules.scout.models import SessionBrief
from modules.scout.storage import save_prediction, get_recent_predictions

BANNER = """
╔══════════════════════════════════════════════════╗
║  SCOUT v1.0 — Football Prediction Intelligence   ║
║  Statistical Computation & Odds Understanding    ║
║  Terminal | The One System | MiroFish Pattern    ║
╚══════════════════════════════════════════════════╝
"""

MARKET_OPTIONS = {
    "1": "W/D/L only",
    "2": "W/D/L + secondary markets (BTTS, Over/Under 2.5)",
}


def interactive_session_brief() -> SessionBrief:
    print(BANNER)
    print("SCOUT SESSION BRIEF\n")
    print("1. Which match do you want analyzed?")
    print("   Format: Home Team vs Away Team | Competition | Date | Kickoff Time")
    match = input("   > ").strip()
    if not match:
        print("No match provided. Exiting.")
        sys.exit(1)

    print("\n2. Primary market focus?")
    print("   [1] W/D/L only")
    print("   [2] W/D/L + secondary markets (BTTS, Over/Under 2.5)")
    market_choice = input("   > ").strip()
    market = MARKET_OPTIONS.get(market_choice, "W/D/L only")

    print("\n3. Any context to factor in?")
    print("   (Injuries, suspensions, rivalry weight, must-win — leave blank if none)")
    context = input("   > ").strip()

    print("\nSCOUT deploying...\n")
    return SessionBrief(match=match, market_focus=market, context=context)


def run_analysis(brief: SessionBrief):
    agent = ScoutAgent()
    print("Gathering intelligence across all four data sources...")
    print("Running MiroFish: Seed → Analyze → Conflict Detection → Verdict\n")

    prediction = agent.analyze(brief)

    # ICE MODE output
    print(prediction.ice_display())

    # FLAMZE MODE output
    print("\nANALYST BREAKDOWN — FLAMZE MODE")
    print("─" * 50)
    print(prediction.flamze_breakdown)
    print()

    # Persist
    pred_id = save_prediction(prediction)
    print(f"[LOGGED] Prediction saved to supernova.db — ID: {pred_id[:8]}")

    return prediction


def show_history():
    rows = get_recent_predictions(10)
    if not rows:
        print("No predictions on record yet.")
        return

    print(f"\n{'Match':<35} {'Confidence':<8} {'Pick':<20} {'Odds':<8} {'Date'}")
    print("─" * 100)
    for r in rows:
        print(f"{r['match']:<35} {r['confidence']:<8} {r['pick']:<20} {r['odds']:<8} {r['date'][:10]}")


def main():
    parser = argparse.ArgumentParser(description="SCOUT Football Prediction CLI")
    parser.add_argument("--match", type=str, help="Match in format 'Home vs Away | Competition | Date | Time'")
    parser.add_argument("--market", type=str, default="W/D/L only", help="Market focus")
    parser.add_argument("--context", type=str, default="", help="Match context (injuries, etc.)")
    parser.add_argument("--history", action="store_true", help="Show recent predictions")
    args = parser.parse_args()

    if args.history:
        show_history()
        return

    if args.match:
        brief = SessionBrief(match=args.match, market_focus=args.market, context=args.context)
        print(BANNER)
    else:
        brief = interactive_session_brief()

    run_analysis(brief)


if __name__ == "__main__":
    main()
