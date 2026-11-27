"""
Forward-test sygnałów screenera SMI na PRAWDZIWYCH dziennych snapshotach listy głównej
(results/results_main_YYYY-MM-DD.json) – bez look-ahead: każdy sygnał jest oceniany cenami
z dni PO publikacji listy.

Zdarzenie (sygnał):
  spółka pojawia się na liście głównej, a nie było jej w COOLDOWN_SNAPSHOTS poprzednich snapshotach.
  Cechy (signal, fuzzy, MTF, Wyckoff, ...) – z dnia pierwszego pojawienia się.
Wejście:
  cena zamknięcia w dniu snapshotu (skan o 11:00 UTC -> kupno w trakcie sesji tego dnia).
Wynik po 4 / 8 / 12 tygodniach (20 / 40 / 60 sesji):
  - zwrot "trzymaj" (close po H sesjach / wejście - 1)
  - zwrot vs benchmark rynku (SPY dla USA, VGK dla EU) w tych samych datach
  - zwrot z Stop Lossem ze screenera (ryzyko % z dnia sygnału przeniesione na cenę wejścia;
    luka poniżej SL -> wyjście po otwarciu)
  - MFE / MAE (maks. zysk / maks. obsunięcie w trakcie)
Grupy: typ sygnału, rynek, fuzzy rescue, MTF, Wyckoff, tech score, dywergencja, wolumen,
Monthly Risk, D1 Lead, discount 52W, brak fundamentów (FUND ?), reżim rynku (benchmark vs SMA50W).

Wyjście: results/forward_test.json + results/forward_test.html (bez CDN, ten sam motyw co dashboard).
Uruchamianie: workflow forward-test.yml (sobota) albo ręcznie: python scripts/forward_test.py
"""

import glob
import html as _html
import json
import math
import os
import re
from datetime import datetime, timezone

import numpy as np
import pandas as pd

RESULTS_DIR          = "results"
SNAPSHOT_GLOB        = "results_main_*.json"
COOLDOWN_SNAPSHOTS   = 10          # ~2 tygodnie przerwy = nowy sygnał tej samej spółki
HORIZONS             = {"4 tyg.": 20, "8 tyg.": 40, "12 tyg.": 60}
BENCH                = {"USA": "SPY", "EU": "VGK"}   # jak MARKET_DIRECTION_* w screener.py
SMA_WEEKS            = 50
MIN_N_RELIABLE       = 20          # poniżej – wynik oznaczony jako mało wiarygodny
DOWNLOAD_BATCH       = 150
FUND_KEYS            = ("eps_ttm", "quick_ratio", "roic", "debt_equity", "gross_margin")


# ══════════════════════════════════════════════════════════════
#  1. Zdarzenia z snapshotów
# ══════════════════════════════════════════════════════════════

def load_snapshots(results_dir=RESULTS_DIR):
    snaps = []
    for path in sorted(glob.glob(os.path.join(results_dir, SNAPSHOT_GLOB))):
        m = re.search(r"(\d{4}-\d{2}-\d{2})\.json$", path)
        if not m:
            continue
        try:
            rows = json.load(open(path, encoding="utf-8"))
        except Exception as e:
            print(f"  pominięty {path}: {e}")
            continue
        if isinstance(rows, dict):
            rows = rows.get("results", [])
        snaps.append((m.group(1), [r for r in rows if r.get("ticker")]))
    return snaps


def build_events(snaps, cooldown=COOLDOWN_SNAPSHOTS):
    """Pierwsze pojawienie się spółki na liście po >= cooldown snapshotach nieobecności."""
    last_seen = {}
    events = []
    for idx, (date, rows) in enumerate(snaps):
        for r in rows:
            t = r["ticker"]
            prev = last_seen.get(t)
            if prev is None or idx - prev > cooldown:
                events.append({"date": date, "ticker": t, "row": r})
            last_seen[t] = idx
    return events


# ══════════════════════════════════════════════════════════════
#  2. Ceny
# ══════════════════════════════════════════════════════════════

def download_prices(tickers, start):
    """{ticker: DataFrame[Open,High,Low,Close]} – dzienne, auto_adjust jak w screenerze."""
    import yfinance as yf
    out = {}
    tickers = sorted(set(tickers))
    for i in range(0, len(tickers), DOWNLOAD_BATCH):
        batch = tickers[i:i + DOWNLOAD_BATCH]
        try:
            raw = yf.download(batch, start=start, interval="1d", group_by="ticker",
                              auto_adjust=True, progress=False, threads=True)
        except Exception as e:
            print(f"  batch {i}: błąd {e}")
            continue
        for t in batch:
            try:
                df = raw[t] if isinstance(raw.columns, pd.MultiIndex) else raw
                df = df[["Open", "High", "Low", "Close"]].dropna(subset=["Close"])
                if len(df):
                    df.index = pd.to_datetime(df.index).tz_localize(None)
                    out[t] = df
            except Exception:
                pass
        print(f"  ceny: {min(i + DOWNLOAD_BATCH, len(tickers))}/{len(tickers)}")
    return out


# ══════════════════════════════════════════════════════════════
#  3. Wynik pojedynczego zdarzenia
# ══════════════════════════════════════════════════════════════

def _entry_index(df, date):
    """Pierwsza sesja >= data snapshotu."""
    pos = df.index.searchsorted(pd.Timestamp(date))
    return int(pos) if pos < len(df) else None


def bench_regime(bench_df, date):
    """True/False: benchmark nad SMA50W w dniu sygnału (tylko dane do tego dnia)."""
    if bench_df is None:
        return None
    w = bench_df["Close"].resample("W-FRI").last().dropna()
    w = w[w.index <= pd.Timestamp(date) + pd.Timedelta(days=6)]
    if len(w) < SMA_WEEKS:
        return None
    return bool(w.iloc[-1] > w.iloc[-SMA_WEEKS:].mean())


ATR_LEN, ATR_STOP_MULT = 14, 2.0      # jak w screener.py (calc_atr_stop)


def rebuild_stop_ratio(df, i0):
    """
    SL dla sygnałów sprzed pola stop_loss (snapshoty < 2026-11-19): ta sama reguła co
    calc_atr_stop w screenerze – max(Close - 2*ATR14 W1, dołek 4 tyg. * 0.99) – liczona
    WYŁĄCZNIE na zamkniętych świecach W1 sprzed tygodnia sygnału (bez look-ahead).
    Zwraca SL / Close_W1 (mnożnik do ceny wejścia) albo None.
    """
    day = df.index[i0]
    week_start = day - pd.Timedelta(days=day.weekday())
    d = df[df.index < week_start]
    w = d.resample("W-FRI").agg({"High": "max", "Low": "min", "Close": "last"}).dropna()
    if len(w) < ATR_LEN + 2:
        return None
    h, l, c = w["High"], w["Low"], w["Close"]
    tr = pd.concat([h - l, (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
    atr = float(tr.ewm(alpha=1 / ATR_LEN, adjust=False).mean().iloc[-1])
    close = float(c.iloc[-1])
    if not np.isfinite(atr) or close <= 0:
        return None
    sl_atr = close - ATR_STOP_MULT * atr
    sl = max(sl_atr, float(l.iloc[-4:].min()) * 0.99)
    if sl >= close:
        sl = sl_atr
    return sl / close if sl > 0 else None


def evaluate_event(ev, prices, benches):
    r = ev["row"]
    df = prices.get(ev["ticker"])
    if df is None or not len(df):
        return None
    i0 = _entry_index(df, ev["date"])
    if i0 is None:
        return None
    entry = float(df["Close"].iloc[i0])
    if not entry > 0:
        return None
    market = r.get("market") or "USA"
    bdf = benches.get(BENCH.get(market, "SPY"))

    # SL: ryzyko % z dnia sygnału przeniesione na cenę wejścia (odporne na korekty dywidend)
    stop, sl_src = None, None
    price_snap, sl = r.get("price"), r.get("stop_loss")
    if sl and price_snap and 0 < sl < price_snap:
        stop, sl_src = entry * (sl / price_snap), "screener"
    else:
        ratio = rebuild_stop_ratio(df, i0)
        if ratio and 0 < ratio < 1:
            stop, sl_src = entry * ratio, "odtworzony"

    out = {"date": ev["date"], "ticker": ev["ticker"], "market": market, "entry": round(entry, 4),
           "stop": round(stop, 4) if stop else None, "sl_source": sl_src, "h": {}}
    for label, h in HORIZONS.items():
        i1 = i0 + h
        if i1 >= len(df):
            continue                      # jeszcze nie dojrzało
        win = df.iloc[i0 + 1:i1 + 1]
        exit_c = float(df["Close"].iloc[i1])
        ret = exit_c / entry - 1
        mfe = float(win["High"].max()) / entry - 1
        mae = float(win["Low"].min()) / entry - 1
        # Stop Loss
        sl_hit, trade = False, ret
        if stop:
            hit = win[win["Low"] <= stop]
            if len(hit):
                d = hit.iloc[0]
                fill = min(float(d["Open"]), stop) if d["Open"] == d["Open"] else stop
                sl_hit, trade = True, fill / entry - 1
        # benchmark w tych samych datach
        excess = None
        if bdf is not None:
            b0 = _entry_index(bdf, df.index[i0])
            b1 = bdf.index.searchsorted(df.index[i1], side="right") - 1
            if b0 is not None and 0 <= b1 and b1 > b0:
                bret = float(bdf["Close"].iloc[b1]) / float(bdf["Close"].iloc[b0]) - 1
                excess = ret - bret
        out["h"][label] = {"ret": ret, "excess": excess, "trade": trade, "sl_hit": sl_hit,
                           "mfe": mfe, "mae": mae}
    out["regime_up"] = bench_regime(bdf, ev["date"])
    return out


# ══════════════════════════════════════════════════════════════
#  4. Grupy i statystyki
# ══════════════════════════════════════════════════════════════

def _b(v, edges, labels, none="brak danych"):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return none
    for e, l in zip(edges, labels):
        if v <= e:
            return l
    return labels[-1]


def _yn(v):
    return "brak danych" if v is None else ("Tak" if v else "Nie")


DIMENSIONS = [
    ("Wszystkie sygnały",       lambda r, e: "Wszystkie"),
    ("Typ sygnału",             lambda r, e: r.get("signal") or "brak danych"),
    ("Rynek",                   lambda r, e: r.get("market") or "brak danych"),
    ("Reżim rynku (SMA50W)",    lambda r, e: {True: "Benchmark nad SMA50W", False: "Benchmark pod SMA50W"}
                                              .get(e.get("regime_up"), "brak danych")),
    ("Fuzzy rescue",            lambda r, e: _yn(r.get("fuzzy_rescue")) if "fuzzy_rescue" in r else "brak danych"),
    ("MTF Score",               lambda r, e: _b(r.get("mtf_score"), [1, 3], ["0–1", "2–3", "4–5"])),
    ("Wyckoff W1",              lambda r, e: "Dystrybucja ⚠" if r.get("wyckoff_dist")
                                              else _b(r.get("wyckoff_score"), [1, 3], ["0–1", "2–3", "4+"])),
    ("Tech score",              lambda r, e: _b(r.get("tech_score"), [5, 8], ["0–5", "6–8", "9+"])),
    ("Dywergencja byka",        lambda r, e: _yn(r.get("divergence_bull"))),
    ("Potwierdzenie wolumenu",  lambda r, e: _yn(r.get("vol_confirm"))),
    ("Monthly Risk",            lambda r, e: _yn(r.get("m_risk_warning")) if "m_risk_warning" in r else "brak danych"),
    ("D1 Lead (cross D1 ≤10 dni)", lambda r, e: "brak danych" if r.get("d1_last_cross_days_ago") is None
                                              else ("Tak" if r["d1_last_cross_days_ago"] <= 10 else "Nie")),
    ("Discount od 52W High",    lambda r, e: _b(r.get("discount_52w"), [40, 60], ["30–40%", "40–60%", "60%+"])),
    ("Fundamenty",              lambda r, e: "Brak (FUND ?)" if all(r.get(k) is None for k in FUND_KEYS)
                                              else "Są dane"),
]


def _stats(vals):
    a = np.array([v for v in vals if v is not None], dtype=float)
    if not len(a):
        return None
    n = len(a)
    sd = float(a.std(ddof=1)) if n > 1 else 0.0
    t = float(a.mean() / (sd / math.sqrt(n))) if n > 1 and sd > 0 else None
    return {"n": n, "mean": float(a.mean()), "median": float(np.median(a)),
            "win": float((a > 0).mean()), "t": t}


def group_stats(evaluated, events_by_key):
    groups = []
    for dim, fn in DIMENSIONS:
        buckets = {}
        for e in evaluated:
            row = events_by_key[(e["date"], e["ticker"])]
            buckets.setdefault(fn(row, e), []).append(e)
        for bucket, items in buckets.items():
            entry = {"dim": dim, "bucket": bucket, "n_events": len(items), "h": {}}
            for label in HORIZONS:
                hs = [x["h"][label] for x in items if label in x["h"]]
                if not hs:
                    continue
                ret = _stats([x["ret"] for x in hs])
                trade = _stats([x["trade"] for x in hs])
                exc = _stats([x["excess"] for x in hs])
                gains = sum(x["trade"] for x in hs if x["trade"] > 0)
                losses = -sum(x["trade"] for x in hs if x["trade"] < 0)
                entry["h"][label] = {
                    "n": ret["n"], "ret": ret, "trade": trade, "excess": exc,
                    "sl_rate": float(np.mean([x["sl_hit"] for x in hs])),
                    "pf": (gains / losses) if losses > 0 else None,
                    "mfe": float(np.mean([x["mfe"] for x in hs])),
                    "mae": float(np.mean([x["mae"] for x in hs])),
                }
            groups.append(entry)
    order = {d: i for i, (d, _) in enumerate(DIMENSIONS)}
    tail = {"brak danych": 2, "Dystrybucja ⚠": 1}
    groups.sort(key=lambda g: (order[g["dim"]], tail.get(g["bucket"], 0), g["bucket"]))
    return groups


# ══════════════════════════════════════════════════════════════
#  5. HTML
# ══════════════════════════════════════════════════════════════

CSS = """
:root{--bg:#080a14;--bg2:#0e1020;--bg3:#131733;--border:#1c2040;--text:#c8cde8;--muted:#6a7096;
--accent:#7c9ef0;--pos:#3ecf8e;--neg:#ff4560;--warn:#ffb800}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);
font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}
.wrap{max-width:1180px;margin:0 auto;padding:1.5rem 1rem 3rem}
h1{font-size:1.35rem;margin:0 0 .25rem}h2{font-size:1rem;margin:2rem 0 .6rem;color:var(--accent)}
.sub{color:var(--muted);font-size:.82rem}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:.7rem;margin:1.2rem 0}
.kpi{background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:.8rem 1rem}
.kpi .l{color:var(--muted);font-size:.72rem;text-transform:uppercase;letter-spacing:.04em}
.kpi .v{font-size:1.45rem;font-weight:700;margin-top:.15rem}.kpi .s{color:var(--muted);font-size:.75rem}
.tabs{display:flex;gap:.4rem;margin:1rem 0;flex-wrap:wrap}
.tab{background:var(--bg2);border:1px solid var(--border);color:var(--text);padding:.45rem .9rem;
border-radius:6px;cursor:pointer;font:inherit}.tab.on{border-color:var(--accent);color:#fff;background:var(--bg3)}
.metric{display:flex;gap:.4rem;flex-wrap:wrap;margin-bottom:.6rem}
.metric label{font-size:.8rem;color:var(--muted);cursor:pointer}
table{width:100%;border-collapse:collapse;background:var(--bg2);border:1px solid var(--border);
border-radius:8px;overflow:hidden;font-variant-numeric:tabular-nums}
th,td{padding:.42rem .6rem;border-bottom:1px solid var(--border);text-align:right;white-space:nowrap}
th{font-size:.7rem;text-transform:uppercase;letter-spacing:.04em;color:var(--muted);background:var(--bg3)}
td:first-child,th:first-child{text-align:left}tr:last-child td{border-bottom:0}
tr:hover td{background:#151a38}
.dim td{background:#0b0d1c;color:var(--accent);font-weight:700;text-align:left;font-size:.8rem}
.pos{color:var(--pos)}.neg{color:var(--neg)}.mut{color:var(--muted)}
.lown{color:var(--warn);font-size:.68rem;margin-left:.3rem;cursor:help}
.barcell{width:150px}.bar{position:relative;height:10px;background:#0b0d1c;border-radius:2px}
.bar i{position:absolute;top:0;height:10px;border-radius:2px}
.bar b{position:absolute;left:50%;top:-2px;width:1px;height:14px;background:#454a6a}
.note{background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:.8rem 1rem;
font-size:.82rem;color:var(--muted);margin-top:1.5rem}.note b{color:var(--text)}
.scroll{overflow-x:auto}
@media (max-width:640px){
 #tbl tr>*:nth-child(3),#tbl tr>*:nth-child(5),#tbl tr>*:nth-child(9),
 #tbl tr>*:nth-child(10),#tbl tr>*:nth-child(11){display:none}
 #tbl .dim td{display:table-cell!important}
 th,td{padding:.4rem .4rem;font-size:.8rem}th{font-size:.62rem}
 #tbl td:first-child{white-space:normal;min-width:6rem}.kpi .v{font-size:1.2rem}}
"""


def _pct(v, digits=1, sign=True):
    if v is None:
        return '<span class="mut">--</span>'
    cls = "pos" if v > 0 else "neg" if v < 0 else "mut"
    s = f"{v * 100:+.{digits}f}%" if sign else f"{v * 100:.{digits}f}%"
    return f'<span class="{cls}">{s}</span>'


def render_html(payload):
    gen = payload["generated_at"]
    labels = list(HORIZONS)
    data_json = json.dumps(payload["groups"], ensure_ascii=False)
    allg = next((g for g in payload["groups"] if g["dim"] == "Wszystkie sygnały"), None)
    kpis = (f'<div class="kpi"><div class="l">Sygnały (zdarzenia)</div><div class="v">{payload["n_events"]}</div>'
            f'<div class="s">{payload["n_evaluated"]} z cenami · {payload["first_date"]} – {payload["last_date"]}</div></div>')
    if allg:
        for lb in labels:
            h = allg["h"].get(lb)
            if not h:
                kpis += (f'<div class="kpi"><div class="l">Po {lb}</div><div class="v mut">--</div>'
                         f'<div class="s">jeszcze nie dojrzało</div></div>')
                continue
            kpis += (f'<div class="kpi"><div class="l">Po {lb} · śr. zwrot z SL</div>'
                     f'<div class="v">{_pct(h["trade"]["mean"])}</div>'
                     f'<div class="s">n={h["n"]} · trafność {h["trade"]["win"] * 100:.0f}% · '
                     f'vs rynek {_pct(h["excess"]["mean"]) if h["excess"] else "--"}</div></div>')
    tabs = "".join(f'<button class="tab{" on" if i == 0 else ""}" data-h="{_html.escape(lb)}">{_html.escape(lb)}</button>'
                   for i, lb in enumerate(labels))
    return f"""<!DOCTYPE html><html lang="pl"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Forward-test sygnałów SMI</title><style>{CSS}</style></head><body><div class="wrap">
<h1>Forward-test sygnałów screenera SMI</h1>
<div class="sub">Wygenerowano {gen} · sygnały z dziennych snapshotów listy głównej, oceniane cenami z dni po publikacji</div>
<div class="kpis">{kpis}</div>
<div class="tabs" id="tabs">{tabs}</div>
<div class="metric" id="metric">
 <label><input type="radio" name="m" value="trade" checked> Zwrot z Stop Lossem</label>
 <label><input type="radio" name="m" value="ret"> Zwrot „trzymaj”</label>
 <label><input type="radio" name="m" value="excess"> Nadwyżka vs rynek</label>
</div>
<div class="scroll"><table id="tbl"></table></div>
<div class="note">
<b>Jak czytać.</b> Jeden wiersz = grupa sygnałów. <b>Śr.</b> i <b>Mediana</b> – zwrot w wybranym horyzoncie;
<b>Trafność</b> – % sygnałów na plusie; <b>t</b> – średnia / błąd standardowy (|t| ≥ 2 ≈ wynik raczej nieprzypadkowy);
<b>SL</b> – % sygnałów, w których Stop Loss został trafiony (z pola stop_loss screenera, a dla sygnałów
sprzed 19.11.2026 odtworzony tą samą regułą: max(Close − 2×ATR14 W1, dołek 4 tyg.), z zamkniętych świec W1 sprzed sygnału); <b>PF</b> – profit factor (z SL);
<b>MFE/MAE</b> – średni maks. zysk / maks. obsunięcie w trakcie. ⚠ = mniej niż {MIN_N_RELIABLE} sygnałów.<br><br>
<b>Ograniczenia.</b> Sygnały nakładają się w czasie (wiele spółek w tym samym tygodniu), więc nie są niezależne –
t-stat zawyża pewność. Reguły screenera zmieniały się w trakcie okresu (pola z nowszych wersji mają „brak danych”
dla starszych sygnałów). Wejście = zamknięcie w dniu publikacji, bez prowizji i poślizgu. Spółki usunięte z giełdy
po sygnale mogą nie mieć cen (pominięte – ryzyko lekkiego zawyżenia wyników). Benchmark EU = VGK (USD).
</div></div>
<script>
const G={data_json};const LOWN={MIN_N_RELIABLE};
let H={json.dumps(labels[0], ensure_ascii=False)}, M="trade";
const f=(v,s=true)=>v==null?'<span class="mut">--</span>':`<span class="${{v>0?'pos':v<0?'neg':'mut'}}">${{s&&v>0?'+':''}}${{(v*100).toFixed(1)}}%</span>`;
function bar(v,mx){{if(v==null)return'';const w=Math.min(50,Math.abs(v)/mx*50);
 const c=v>=0?'var(--pos)':'var(--neg)';const l=v>=0?50:50-w;
 return `<div class="bar" title="${{(v*100).toFixed(1)}}%"><b></b><i style="left:${{l}}%;width:${{w}}%;background:${{c}}"></i></div>`}}
function draw(){{
 const rows=G.filter(g=>g.h[H]);let mx=0.0001;
 rows.forEach(g=>{{const s=g.h[H][M];if(s)mx=Math.max(mx,Math.abs(s.mean))}});
 let h=`<tr><th>Grupa</th><th>n</th><th class="barcell">Śr. (0 = środek)</th><th>Śr.</th><th>Mediana</th><th>Trafność</th><th>t</th><th>SL</th><th>PF</th><th>MFE</th><th>MAE</th></tr>`;
 let dim=null;
 for(const g of rows){{const x=g.h[H],s=x[M];
  if(g.dim!==dim){{dim=g.dim;h+=`<tr class="dim"><td colspan="11">${{dim}}</td></tr>`}}
  const low=x.n<LOWN?`<span class="lown" title="Mniej niż ${{LOWN}} sygnałów – wynik mało wiarygodny">⚠</span>`:'';
  h+=`<tr><td>${{g.bucket}}${{low}}</td><td>${{x.n}}</td><td class="barcell">${{s?bar(s.mean,mx):''}}</td>
  <td>${{s?f(s.mean):'--'}}</td><td>${{s?f(s.median):'--'}}</td><td>${{s?(s.win*100).toFixed(0)+'%':'--'}}</td>
  <td class="${{s&&s.t!=null&&Math.abs(s.t)>=2?'':'mut'}}">${{s&&s.t!=null?s.t.toFixed(1):'--'}}</td>
  <td>${{(x.sl_rate*100).toFixed(0)}}%</td><td>${{x.pf!=null?x.pf.toFixed(2):'--'}}</td>
  <td>${{f(x.mfe)}}</td><td>${{f(x.mae)}}</td></tr>`}}
 document.getElementById('tbl').innerHTML=h}}
document.getElementById('tabs').onclick=e=>{{const b=e.target.closest('.tab');if(!b)return;
 document.querySelectorAll('.tab').forEach(t=>t.classList.toggle('on',t===b));H=b.dataset.h;draw()}};
document.getElementById('metric').onchange=e=>{{M=e.target.value;draw()}};
draw();
</script></body></html>"""


# ══════════════════════════════════════════════════════════════
#  6. Główna funkcja
# ══════════════════════════════════════════════════════════════

def run(results_dir=RESULTS_DIR, price_loader=download_prices):
    snaps = load_snapshots(results_dir)
    if not snaps:
        print("Brak snapshotów results_main_*.json – nic do zrobienia.")
        return None
    events = build_events(snaps)
    print(f"Snapshoty: {len(snaps)} ({snaps[0][0]} – {snaps[-1][0]}), zdarzenia: {len(events)}")
    start = (pd.Timestamp(snaps[0][0]) - pd.Timedelta(days=SMA_WEEKS * 7 + 60)).strftime("%Y-%m-%d")
    tickers = {e["ticker"] for e in events} | set(BENCH.values())
    prices = price_loader(sorted(tickers), start)
    benches = {b: prices.get(b) for b in BENCH.values()}
    evaluated, missing = [], []
    for ev in events:
        res = evaluate_event(ev, prices, benches)
        if res is None:
            missing.append(ev["ticker"])
        else:
            evaluated.append(res)
    if missing:
        print(f"  bez cen: {len(missing)} zdarzeń (np. {', '.join(sorted(set(missing))[:10])})")
    by_key = {(e["date"], e["ticker"]): e["row"] for e in events}
    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "params": {"cooldown_snapshots": COOLDOWN_SNAPSHOTS, "horizons_sessions": HORIZONS,
                   "benchmarks": BENCH, "entry": "close w dniu snapshotu"},
        "first_date": snaps[0][0], "last_date": snaps[-1][0],
        "n_snapshots": len(snaps), "n_events": len(events), "n_evaluated": len(evaluated),
        "n_missing_prices": len(missing),
        "groups": group_stats(evaluated, by_key),
        "events": [{k: v for k, v in e.items()} for e in evaluated],
    }
    os.makedirs(results_dir, exist_ok=True)
    with open(os.path.join(results_dir, "forward_test.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=1, default=float)
    with open(os.path.join(results_dir, "forward_test.html"), "w", encoding="utf-8") as f:
        f.write(render_html(payload))
    allg = next((g for g in payload["groups"] if g["dim"] == "Wszystkie sygnały"), None)
    if allg:
        for lb, h in allg["h"].items():
            print(f"  {lb}: n={h['n']}  śr. z SL {h['trade']['mean'] * 100:+.1f}%  "
                  f"trafność {h['trade']['win'] * 100:.0f}%  SL {h['sl_rate'] * 100:.0f}%")
    print(f"Zapisano {results_dir}/forward_test.html i forward_test.json")
    return payload


if __name__ == "__main__":
    run()
