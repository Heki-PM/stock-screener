#!/usr/bin/env python3
"""
scripts/make_prompt.py

Selekcja spółek do analizy tekstowej (Warstwa 2). Ten plik jest podpięty
BEZ ŻADNYCH zmian w screener.yml — istniejący krok "Generate Claude prompt
file" już go wywołuje warunkowo:

    [ -f scripts/make_prompt.py ] && python scripts/make_prompt.py || true

Uruchamiany po kroku "Save dated copy of results", więc w results/ są już:
  results_main.json              — dzisiejszy stan (main_results, po filter_main)
  results_main_{DZIS}.json        — dzisiejsza kopia (identyczna z powyższym)
  results_main_{wcześniejsza data}.json  — poprzednie dni (do 30 dni wstecz)

Zasada wyboru (regulowana niżej w KONFIGURACJI):
  - TOP N sygnałów "Strong BUY" wg tech_score (przy remisie: mtf_score),
  - + spółki, których signal LUB wyckoff_phase zmieniły się względem
    poprzedniego dostępnego dnia,
  - + spółki nowe w results_main (nie było ich wczoraj wcale).

To NIE jest cały rynek (7000+ tickerów) — tylko finalna lista main_results
(już przefiltrowana przez screener.py), więc selekcja i tak startuje z
dużo mniejszej puli. Bez tego pliku nic się nie zmienia w Twoim workflow —
krok istnieje, ale dziś nic nie robi (`[ -f ... ] || true`).

Wyjście:
  results/to_analyze.json — lista wybranych spółek Z PEŁNYMI DANYMI (nie
  tylko ticker), żeby Cowork Scheduled Task czytał jeden mały plik zamiast
  całego results_main.json. Ten plik trafia do repo tym samym `git add
  results/` co reszta wyników — nic więcej nie trzeba zmieniać w workflow.
"""

from __future__ import annotations

import glob
import json
from datetime import datetime, timezone
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# ---------- KONFIGURACJA (tu regulujesz zasięg/koszt Warstwy 2) ----------
TOP_N_STRONG_BUY = 5           # ile najlepszych Strong BUY dostaje analizę
INCLUDE_SIGNAL_CHANGES = True   # zmiana signal LUB wyckoff_phase vs wczoraj
INCLUDE_NEW_ENTRIES = True      # spółki nowe w results_main
INCLUDE_FUZZY_RESCUE = False    # spółki wpuszczone dzięki fuzzy_rescue — opcjonalnie
MAX_TOTAL_PER_DAY = 15          # twardy limit dzienny (bezpiecznik)
# ---------------------------------------------------------------------------


def load_today() -> list[dict]:
    path = RESULTS_DIR / "results_main.json"
    if not path.exists():
        print(f"BŁĄD: brak {path} — uruchamiać po screener.py")
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def find_previous_snapshot() -> list[dict] | None:
    """Najnowszy results_main_{DATA}.json starszy niż dzisiejszy (korzysta
    z istniejącego mechanizmu datowanych kopii z kroku 'Save dated copy')."""
    candidates = []
    for p in glob.glob(str(RESULTS_DIR / "results_main_*.json")):
        date_part = Path(p).stem.replace("results_main_", "")
        if date_part < TODAY:
            candidates.append((date_part, p))
    if not candidates:
        return None
    candidates.sort()
    _, path = candidates[-1]
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def select(today: list[dict], yesterday: list[dict] | None) -> list[dict]:
    yesterday_by_ticker = {r["ticker"]: r for r in yesterday} if yesterday else {}
    selected: dict[str, str] = {}  # ticker -> powód (pierwszy trafiony wygrywa)

    # 1) TOP N Strong BUY wg tech_score (0-12), przy remisie mtf_score (0-5)
    strong = [r for r in today if r.get("signal") == "Strong BUY"]
    strong.sort(key=lambda r: (r.get("tech_score") or 0, r.get("mtf_score") or 0), reverse=True)
    for r in strong[:TOP_N_STRONG_BUY]:
        selected[r["ticker"]] = "TOP Strong BUY"

    # 2) zmiana sygnału lub fazy Wyckoff względem poprzedniego dnia
    if INCLUDE_SIGNAL_CHANGES:
        for r in today:
            t = r["ticker"]
            prev = yesterday_by_ticker.get(t)
            if not prev:
                continue
            if prev.get("signal") != r.get("signal"):
                selected.setdefault(t, f"Zmiana sygnału: {prev.get('signal')} -> {r.get('signal')}")
            if prev.get("wyckoff_phase") != r.get("wyckoff_phase"):
                selected.setdefault(t, f"Zmiana fazy Wyckoff: {prev.get('wyckoff_phase')} -> {r.get('wyckoff_phase')}")

    # 3) nowe spółki w results_main (nie było ich wczoraj wcale)
    if INCLUDE_NEW_ENTRIES:
        for r in today:
            if r["ticker"] not in yesterday_by_ticker:
                selected.setdefault(r["ticker"], "Nowa spółka w screenerze głównym")

    # 4) opcjonalnie: fuzzy rescue (1 twardy próg niespełniony, ale wysoka jakość łączna)
    if INCLUDE_FUZZY_RESCUE:
        for r in today:
            if r.get("fuzzy_rescue"):
                selected.setdefault(r["ticker"], "Fuzzy rescue")

    by_ticker = {r["ticker"]: r for r in today}
    result = [
        {"ticker": t, "reason": reason, "data": by_ticker.get(t, {})}
        for t, reason in selected.items()
    ]

    if len(result) > MAX_TOTAL_PER_DAY:
        print(f"UWAGA: {len(result)} kandydatów, obcinam do limitu {MAX_TOTAL_PER_DAY}")
        result = result[:MAX_TOTAL_PER_DAY]

    return result


def main() -> None:
    today = load_today()
    if not today:
        print("Brak danych w results_main.json — nic do zrobienia.")
        return

    yesterday = find_previous_snapshot()
    if yesterday is None:
        print("Brak wcześniejszego results_main_*.json — pierwszy dzień, analizuję tylko TOP N.")

    selected = select(today, yesterday)

    out_path = RESULTS_DIR / "to_analyze.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"date": TODAY, "selected": selected}, f, ensure_ascii=False, indent=2)

    print(f"OK: wybrano {len(selected)} spolek do analizy Claude -> {out_path}")
    for s in selected:
        print(f"  - {s['ticker']}: {s['reason']}")


if __name__ == "__main__":
    main()
