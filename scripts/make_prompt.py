"""
scripts/make_prompt.py
Generuje results/paste_to_claude.txt — gotowy prompt do wklejenia w Claude.ai
Brak zewnętrznych zależności (tylko stdlib).
"""
import json
from datetime import datetime
from pathlib import Path

RESULTS_DIR = Path("results")

def load():
    r = json.loads((RESULTS_DIR / "results.json").read_text(encoding="utf-8"))
    m = {}
    mp = RESULTS_DIR / "meta.json"
    if mp.exists():
        m = json.loads(mp.read_text(encoding="utf-8"))
    return r, m

def build(results, meta):
    strong  = [r for r in results if r.get("signal") == "Strong BUY"]
    turning = [r for r in results if r.get("signal") == "Turning Up"]
    date    = meta.get("generated_at", datetime.now().isoformat())[:10]

    def row(r):
        disc = r.get("discount_52w", "?")
        cap  = r.get("market_cap_mln", "?")
        return (
            f"  {r['ticker']:6s} | {r.get('name','')[:28]:28s} | {r.get('sector','?')[:20]:20s} | "
            f"{str(r.get('price','?')):>8} {r.get('currency','USD')} | "
            f"dyskonto -{disc}% | cap {cap}M | "
            f"SMI {r.get('smi','?')} / EMA {r.get('smi_ema','?')} | "
            f"strefa {r.get('zone','?'):12s} | "
            f"EPS {r.get('eps_ttm','?')} | QR {r.get('quick_ratio','?')}"
        )

    lines = [
        "Przeanalizuj poniższe wyniki automatycznego screenera akcji i napisz raport analityczny po polsku.",
        "",
        "=== KONTEKST ===",
        "Wskaźnik: SMI(10,3,3) tygodniowy",
        "Strong BUY  = crossover SMI > EMA ze strefy wyprzedania (SMI poprzednio ≤ -40)",
        "Turning Up  = SMI osiągnął lokalne dno i odbija, jeszcze poniżej EMA (wczesny sygnał)",
        "Filtr wejścia: dyskonto ≥ 30% vs 52-tygodniowego szczytu + EPS>0 + QR>1 + Cap>200M",
        "",
        "=== METADANE SKANU ===",
        f"Data:              {date}",
        f"Przeskanowano:     {meta.get('total_scanned', '?')} spółek",
        f"Sygnały SMI:       {meta.get('weekly_signals', '?')}",
        f"Po wszystkich filtrach: {len(results)} (Strong BUY: {len(strong)}, Turning Up: {len(turning)})",
        "",
        "=== SYGNAŁY STRONG BUY ===",
        f"(posortowane od największego dyskonta; {len(strong)} spółek)",
    ]

    for r in sorted(strong, key=lambda x: -(x.get("discount_52w") or 0)):
        lines.append(row(r))

    lines += [
        "",
        "=== SYGNAŁY TURNING UP ===",
        f"(SMI jeszcze poniżej EMA — wczesny sygnał; {len(turning)} spółek)",
    ]
    for r in sorted(turning, key=lambda x: -(x.get("discount_52w") or 0)):
        lines.append(row(r))

    lines += [
        "",
        "=== OCZEKIWANA STRUKTURA RAPORTU ===",
        "1. Kontekst rynkowy — co ilość/rozkład sygnałów mówi o sentymencie",
        "2. TOP 3 najciekawsze sygnały — techniczne + fundamentalne + ryzyko",
        "3. Analiza sektorowa — dominujące sektory, co sugeruje o rotacji",
        "4. Ryzyka przed zajęciem pozycji",
        "5. Podsumowanie (3–5 zdań)",
        "",
        "Używaj konkretnych liczb z danych. Nie udzielaj rekomendacji inwestycyjnych.",
    ]

    return "\n".join(lines)

if __name__ == "__main__":
    results, meta = load()
    content = build(results, meta)
    out = RESULTS_DIR / "paste_to_claude.txt"
    out.write_text(content, encoding="utf-8")
    date = meta.get("generated_at", "")[:10]
    print(f"✅ Wygenerowano: {out}  ({len(results)} sygnałów, {date})")
    print(f"   Rozmiar: {len(content)} znaków")
