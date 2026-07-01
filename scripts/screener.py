"""
Stock Screener – SMI Tygodniowy
Jeden przebieg generuje dwa raporty:

  results/screener.html   – Screener główny (Strong BUY + Turning Up)
                            Filtry: Cap>200M | Vol>300K | EPS>0 | QR≥1.0 | Discount≥30%
                                    ROIC>15% | Debt/Equity<1 | Gross Margin>30%

  results/index_all.html  – Full Scan (Strong BUY + BUY + Turning Up)
                            Filtry: tylko Cap>200M | Vol>300K

Dane pobierane są raz – ticker list + tygodniowe OHLC + fundamenty.
SMI(10,3,3) – port Pine Script "SMI Signal Strategy"

CHANGELOG:
  FIX 1 – _w_find_spring: okno ar_pos+61 zamiast sc_pos+61 (sc < ar zawsze)
  FIX 2 – filter_main: None przepuszcza, tylko zła wartość odrzuca (oryginalna logika)
  FIX 3 – RS 12M: wzór (1+sr)/(1+ir) zamiast sr/(1+ir)
  FIX 4 – calc_tech_score: ważona punktacja SCORE_WEIGHTS, kara wyk_dist=-3
  FIX 5 – cache fundamentów 24h (katalog .cache/fundamentals/)
  FIX 6 – filtr EPS: eps <= 0 zamiast eps < 0 (break-even nie liczy się jako zysk)
  OPT 1 – pobieranie list tickerów równolegle (4 wątki HTTP jednocześnie)
  OPT 2 – market_direction i phase1_weekly_signals nakładają się w czasie
  OPT 3 – bulk_download: batche 200 (było 100) + równoległe batche (4 wątki)
  OPT 4 – Wyckoff liczony tylko dla spółek z sygnałem SMI (~12% universum)
  OPT 5 – fundamenty 30 wątków (było 20), zapis JSON/CSV/HTML/TV równolegle
  D1 LEAD – pomiar wyprzedzenia SMI dziennego vs tygodniowego + sekcja Early Signal
  MTF SCORE – Multi-TimeFrame Score 0-5 łączący D1+W1+Monthly (tylko finalna lista)
  MONTHLY RISK – ostrzeżenie gdy SMI miesięczny przeczy sygnałowi W1 (Faza 3)
  TV LIST #2   – MTF score i Monthly Risk w komentarzu każdej linii watchlisty
  SORT #4      – sortowanie main_results po Fazie 3: tech_score*2 + mtf_score
  EPS GROWTH #6– calc_tech_score: +1 za EPS QoQ >= +15% (akceleracja zysku)
"""

import yfinance as yf
import pandas as pd
import numpy as np
import requests
import json
import os
from datetime import datetime
from pathlib import Path
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed

# ══════════════════════════════════════════════════════════════
#  KONFIGURACJA
# ══════════════════════════════════════════════════════════════
MIN_MARKET_CAP   = 200_000_000
MIN_VOLUME       = 300_000

MIN_QUICK        = 1.0
MIN_DISCOUNT_52W = 0.30
MIN_ROIC         = 0.15
MAX_DEBT_EQUITY  = 1.0
MIN_GROSS_MARGIN = 0.30

MAX_PRICE        = 150.0
VOL_CONFIRM_MULT = 1.4
RS_MIN_OUTPERFORM= 1.10
MARKET_DIRECTION_USA = "SPY"
MARKET_DIRECTION_EU  = "VGK"
MARKET_SMA_WEEKS     = 50

FUNDAMENTALS_WORKERS = 30   # OPT 5: więcej wątków – fundamenty to I/O-bound
DOWNLOAD_BATCH_SIZE  = 200   # OPT 3: większe batche = mniej round-tripów HTTP
OUTPUT_DIR           = "results"
SMI_LEN_K, SMI_LEN_D, SMI_LEN_EMA = 10, 3, 3

# FIX 5 – cache fundamentów
CACHE_DIR      = Path(".cache/fundamentals")
CACHE_TTL_H    = 24   # godziny ważności cache

# FIX 4 – ważone składniki scoringu technicznego
# Suma maksimum przy wszystkich spełnionych: 4+3+3+2+2+2+1+1+1+1+2 = 22
# Klip do 12 zachowany dla wstecznej kompatybilności wyświetlania.
SCORE_WEIGHTS = {
    "strong_buy":       4,   # sygnał podstawowy – najważniejszy
    "buy":              2,
    "oversold":         3,   # strefa ma realny wpływ predykcyjny
    "bearish":          1,
    "divergence":       3,   # rzadkie, wartościowe potwierdzenie
    "vol_confirm":      2,   # potwierdzenie wolumenu
    "roic":             2,   # jakość biznesu
    "eps_positive":     1,
    "eps_growth_strong":1,   # EPS QoQ >= +15% – wzrost nie przez cięcia
    "qr_ok":            1,
    "rs_outperform":    1,
    "wyckoff_high":     2,   # Spring/SOS – silny sygnał akumulacji
    "wyckoff_dist":    -3,   # dystrybucja – mocna kara
}

# MTF (Multi-TimeFrame) Score – punkty 3/4: skala 0-5
# Liczony tylko dla finalnej listy (po filter_main), bo wymaga danych Monthly.
MTF_SCORE_MAX = 5

# ══════════════════════════════════════════════════════════════
#  FIX 5 – CACHE FUNDAMENTÓW
# ══════════════════════════════════════════════════════════════

def _cache_path(symbol: str) -> Path:
    """Zwraca ścieżkę pliku cache dla danego symbolu."""
    # Zastępujemy znaki niedozwolone w nazwach plików
    safe = symbol.replace("/", "_").replace("\\", "_").replace(":", "_")
    return CACHE_DIR / f"{safe}.json"

def _load_cache(symbol: str) -> dict | None:
    """
    Wczytuje dane fundamentalne z cache jeśli istnieją i nie wygasły.
    Zwraca dict lub None gdy brak / wygasły.
    """
    p = _cache_path(symbol)
    if not p.exists():
        return None
    try:
        age_h = (datetime.now().timestamp() - p.stat().st_mtime) / 3600
        if age_h > CACHE_TTL_H:
            return None
        data = json.loads(p.read_text(encoding="utf-8"))
        # Odświeżamy pole scanned_at żeby pokazywało kiedy dane były cache'owane
        data["_from_cache"] = True
        return data
    except Exception:
        return None

def _save_cache(symbol: str, data: dict) -> None:
    """Zapisuje dane fundamentalne do cache."""
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        p = _cache_path(symbol)
        # Nie zapisujemy pola _from_cache do pliku
        to_save = {k: v for k, v in data.items() if k != "_from_cache"}
        p.write_text(json.dumps(to_save, ensure_ascii=False, default=str),
                     encoding="utf-8")
    except Exception:
        pass   # cache nieobowiązkowy – nie przerywamy działania

# ══════════════════════════════════════════════════════════════
#  WYCKOFF – DETEKCJA AKUMULACJI I DYSTRYBUCJI
# ══════════════════════════════════════════════════════════════

def _w_rolling_vol(df, window=20):
    return df["Volume"].rolling(window).mean()

def _w_find_sc(df, vol_avg, lookback=80):
    recent = df.iloc[-lookback:].copy()
    recent["vol_avg"]    = vol_avg.iloc[-lookback:].values
    recent["range"]      = recent["High"] - recent["Low"]
    recent["lower_wick"] = recent[["Open", "Close"]].min(axis=1) - recent["Low"]
    cand = recent[
        (recent["Volume"]      > 2.0 * recent["vol_avg"]) &
        (recent["Close"]       < recent["Open"]) &
        (recent["lower_wick"]  > 0.25 * recent["range"])
    ]
    if cand.empty:
        return None
    idx = cand["Close"].idxmin()
    row = df.loc[idx]
    return {"idx": idx, "price": float(row["Low"]), "close": float(row["Close"]),
            "vol_ratio": float(row["Volume"] / vol_avg.loc[idx])}

def _w_find_ar(df, sc, max_bars=8):
    pos = df.index.get_loc(sc["idx"])
    win = df.iloc[pos+1 : pos+1+max_bars]
    if win.empty:
        return None
    idx   = win["High"].idxmax()
    price = float(df.loc[idx, "High"])
    return {"idx": idx, "price": price} if price >= sc["close"] * 1.05 else None

def _w_find_tr(df, sc, ar, min_bars=6):
    pos = df.index.get_loc(ar["idx"])
    win = df.iloc[pos+1 : pos+1+40]
    if len(win) < min_bars:
        return None
    sup, res = sc["close"], ar["price"]
    rng = res - sup
    if rng <= 0:
        return None
    in_r = win[(win["Close"] >= sup - 0.12*rng) & (win["Close"] <= res + 0.12*rng)]
    if len(in_r) < min_bars:
        return None
    return {"support": sup, "resistance": res,
            "range_pct": rng/sup*100, "bars_in_range": len(in_r)}

def _w_find_spring(df, sc, ar, vol_avg):
    # FIX 1 – SC zawsze poprzedza AR (sc_pos < ar_pos), więc
    # min(sc_pos+61, len(df)) dawało okno KRÓTSZE niż ar_pos+1 → pusty search.
    # Poprawka: okno zaczyna się za AR i sięga 60 świec do przodu.
    ar_pos = df.index.get_loc(ar["idx"])
    search = df.iloc[ar_pos+1 : min(ar_pos+61, len(df))]
    sup    = sc["close"]
    for idx, row in search.iterrows():
        if row["Low"] < sup*0.992 and row["Close"] > sup*0.994:
            vv  = vol_avg.loc[idx]
            vr  = row["Volume"] / vv if vv > 0 else 1.0
            return {"idx": idx, "low": float(row["Low"]),
                    "close": float(row["Close"]),
                    "vol_ratio": float(vr),
                    "type": "strong" if vr > 1.5 else "weak"}
    return None

def _w_find_sos(df, ar, spring, vol_avg):
    ref = spring["idx"] if spring else ar["idx"]
    pos = df.index.get_loc(ref)
    for idx, row in df.iloc[pos+1 : pos+41].iterrows():
        if row["Close"] > ar["price"]*1.008:
            vv = vol_avg.loc[idx]
            vr = row["Volume"]/vv if vv > 0 else 1.0
            if vr > 1.4:
                return {"idx": idx, "price": float(row["Close"]), "vol_ratio": float(vr)}
    return None

def _w_check_dist(df, vol_avg, lookback=60):
    recent = df.iloc[-lookback:].copy()
    rv     = vol_avg.iloc[-lookback:]
    recent["vr"]  = recent["Volume"] / rv.values
    recent["uw"]  = recent["High"] - recent[["Open","Close"]].max(axis=1)
    recent["rng"] = recent["High"] - recent["Low"]
    count, signals = 0, []

    # Sygnał 1: Buying Climax
    rh   = recent["High"].rolling(20).max()
    cand = recent[
        (recent["Close"] > recent["Open"]) &
        (recent["vr"]    > 2.0) &
        (recent["uw"]    > 0.2 * recent["rng"]) &
        (recent["High"]  >= rh * 0.97)
    ]
    if not cand.empty:
        r = cand.iloc[-1]
        count += 1
        signals.append(f"BC przy {r['Close']:.2f} (vol {r['vr']:.1f}x)")

    # Sygnał 2: Upthrust
    rres = recent["High"].shift(1).rolling(20).max()
    cand = recent[
        (recent["High"]  > rres * 1.005) &
        (recent["Close"] < rres) &
        (recent["vr"]    > 1.3)
    ]
    if not cand.empty:
        r = cand.iloc[-1]
        count += 1
        signals.append(f"UT High={r['High']:.2f} Close={r['Close']:.2f} (vol {r['vr']:.1f}x)")

    # Sygnał 3: Wolumen przy spadkach > wzrostach
    last20 = recent.iloc[-20:]
    dn = last20[last20["Close"] < last20["Open"]]
    up = last20[last20["Close"] > last20["Open"]]
    if len(dn) >= 3 and len(up) >= 3:
        avg_dn, avg_up = dn["Volume"].mean(), up["Volume"].mean()
        if avg_dn > avg_up * 1.3:
            count += 1
            signals.append(f"SOW: vol spadkowy {avg_dn/avg_up:.1f}x > wzrostowy")

    return {"count": count, "warning": count >= 2, "signals": signals}

def wyckoff_score(df):
    """
    Oblicza scoring akumulacji Wyckoffa (0–5) i flagę dystrybucji.
    Przyjmuje DataFrame W1 z kolumnami Open/High/Low/Close/Volume.
    Zwraca dict: score, phase, dist_warning, dist_signals.
    """
    out = {"score": 0, "phase": "–",
           "dist_warning": False, "dist_signals": []}

    req = {"Open","High","Low","Close","Volume"}
    if len(df) < 30 or not req.issubset(df.columns):
        return out

    vol_avg = _w_rolling_vol(df)

    # Dystrybucja (zawsze)
    dist = _w_check_dist(df, vol_avg)
    out["dist_warning"] = dist["warning"]
    out["dist_signals"] = dist["signals"]

    # Akumulacja
    sc = _w_find_sc(df, vol_avg)
    if not sc:
        return out
    out["score"] += 1; out["phase"] = "A"

    ar = _w_find_ar(df, sc)
    if not ar:
        return out
    out["score"] += 1

    tr = _w_find_tr(df, sc, ar)
    if not tr:
        return out
    out["score"] += 1; out["phase"] = "B"

    spring = _w_find_spring(df, sc, ar, vol_avg)
    if spring:
        out["score"] += 1; out["phase"] = "C"

    sos = _w_find_sos(df, ar, spring, vol_avg)
    if sos:
        out["score"] += 1; out["phase"] = "D"

    return out

def _wyckoff_cell(score, dist_warning):
    """Zwraca (etykieta, kolor_tła) do komórki HTML."""
    if dist_warning:
        return "⚠ Dist", "#3d0a0a"
    labels = {0:"–", 1:"SC", 2:"A+AR", 3:"Faza B", 4:"Spring", 5:"SOS"}
    colors = {0:"", 1:"#2a2200", 2:"#2a2200", 3:"#0a2200", 4:"#0a2e10", 5:"#0a2e10"}
    return labels.get(score,"–"), colors.get(score,"")

# ══════════════════════════════════════════════════════════════
#  O'NEIL – KIERUNEK RYNKU
# ══════════════════════════════════════════════════════════════

def check_market_direction() -> dict:
    result = {"USA": True, "EU": True,
              "usa_price": None, "usa_sma50w": None,
              "eu_price":  None, "eu_sma50w":  None}
    for key, ticker in [("USA", MARKET_DIRECTION_USA), ("EU", MARKET_DIRECTION_EU)]:
        try:
            df = yf.download(ticker, period="3y", interval="1wk",
                             auto_adjust=True, progress=False)
            if df is None or df.empty or len(df) < MARKET_SMA_WEEKS + 5:
                continue
            close  = df["Close"].dropna()
            price  = float(close.iloc[-1])
            sma50w = float(close.rolling(MARKET_SMA_WEEKS).mean().iloc[-1])
            above  = price > sma50w
            result[key] = above
            pk = key.lower()
            result[f"{pk}_price"]  = round(price,  2)
            result[f"{pk}_sma50w"] = round(sma50w, 2)
            status = "✓ ABOVE" if above else "✗ BELOW"
            print(f"  [Market Direction] {ticker}: {price:.2f} vs SMA50W {sma50w:.2f}  {status}")
        except Exception as e:
            print(f"  [Market Direction] {ticker}: blad ({e})")
    return result

# ══════════════════════════════════════════════════════════════
#  TICKERY
# ══════════════════════════════════════════════════════════════

def get_sp500():
    try:
        url = ("https://raw.githubusercontent.com/datasets/"
               "s-and-p-500-companies/main/data/constituents.csv")
        r  = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        df = pd.read_csv(StringIO(r.text))
        tickers = (df["Symbol"].dropna().str.strip()
                   .str.replace(".", "-", regex=False).tolist())
        print(f"  S&P 500: {len(tickers)}"); return tickers
    except Exception as e:
        print(f"  S&P 500 blad: {e}"); return []

def get_nasdaq():
    import re
    try:
        url = ("https://raw.githubusercontent.com/rreichel3/"
               "US-Stock-Symbols/main/nasdaq/nasdaq_tickers.json")
        r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        tickers = [t.strip() for t in r.json() if re.match(r'^[A-Z]{1,5}$', t.strip())]
        print(f"  NASDAQ: {len(tickers)}"); return tickers
    except Exception as e:
        print(f"  NASDAQ blad: {e}"); return []

def get_nyse_amex():
    import re
    tickers = []
    for exchange in ("nyse", "amex"):
        try:
            url = (f"https://raw.githubusercontent.com/rreichel3/"
                   f"US-Stock-Symbols/main/{exchange}/{exchange}_tickers.json")
            r    = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
            part = [t.strip() for t in r.json() if re.match(r'^[A-Z]{1,5}$', t.strip())]
            tickers.extend(part)
            print(f"  {exchange.upper()}: {len(part)}")
        except Exception as e:
            print(f"  {exchange.upper()} blad: {e}")
    return tickers

def get_european_indices():
    dax = [
        "ADS.DE","AIR.DE","ALV.DE","BAS.DE","BAYN.DE","BEI.DE","BMW.DE","BNR.DE",
        "CON.DE","1COV.DE","DHER.DE","DB1.DE","DBK.DE","DHL.DE","DTE.DE","EOAN.DE",
        "FRE.DE","FME.DE","HEI.DE","HEN3.DE","IFX.DE","LIN.DE","MBG.DE","MRK.DE",
        "MTX.DE","MUV2.DE","PAH3.DE","POWR.DE","QGEN.DE","RHM.DE","RWE.DE","SAP.DE",
        "SHL.DE","SIE.DE","SY1.DE","VNA.DE","VOW3.DE","ZAL.DE","PUM.DE","ENR.DE",
    ]
    cac = [
        "AC.PA","ACA.PA","AI.PA","AIR.PA","ALO.PA","MT.PA","ATO.PA","CS.PA","BNP.PA",
        "EN.PA","CAP.PA","CA.PA","AXA.PA","DSY.PA","EDEN.PA","EL.PA","ERF.PA","EDF.PA",
        "ENGI.PA","FP.PA","KER.PA","LR.PA","LHN.PA","MC.PA","ML.PA","ORA.PA","RI.PA",
        "PUB.PA","RNO.PA","SAF.PA","SGO.PA","SAN.PA","SU.PA","GLE.PA","STLAM.PA",
        "STM.PA","TEP.PA","HO.PA","URW.PA","VIE.PA","DG.PA","VIV.PA","WLN.PA",
    ]
    ftse = [
        "AAF.L","AAL.L","ABF.L","ADM.L","AHT.L","ANTO.L","AZN.L","AUTO.L","AV.L",
        "BAB.L","BA.L","BARC.L","BATS.L","BHKLY.L","BP.L","BDEV.L","BKG.L","BLND.L",
        "BT-A.L","CCH.L","CNA.L","CPG.L","CRDA.L","DCC.L","DGE.L","EXPN.L","FERG.L",
        "FLTR.L","FRES.L","GSK.L","GLEN.L","HLMA.L","HL.L","HSBA.L","IMB.L","INF.L",
        "IHG.L","III.L","ITRK.L","JD.L","JMAT.L","KGF.L","LAND.L","LGEN.L","LLOY.L",
        "LMP.L","MKS.L","MNDI.L","MNG.L","MRO.L","NG.L","NXT.L","OCDO.L","PHNX.L",
        "PRU.L","PSH.L","PSN.L","PSON.L","REL.L","RIO.L","RKT.L","RMV.L","RR.L",
        "RS1.L","SBRY.L","SDR.L","SGE.L","SHEL.L","SKG.L","SKY.L","SLA.L","SMDS.L",
        "SMIN.L","SMT.L","SN.L","SPX.L","SSE.L","STAN.L","SVT.L","TSCO.L","TW.L",
        "ULVR.L","UTG.L","UU.L","VOD.L","WEIR.L","WPP.L","WTB.L",
    ]
    aex = [
        "ABN.AS","ADYEN.AS","AGN.AS","AH.AS","AKZA.AS","MT.AS","ASML.AS","ASR.AS",
        "BESI.AS","DSMF.AS","EXOR.AS","HEIA.AS","IMCD.AS","INGA.AS","JUST.AS",
        "KPN.AS","NN.AS","PHIA.AS","PRX.AS","RAND.AS","REN.AS","SHELL.AS","SBM.AS",
        "URW.AS","UNA.AS","VPK.AS","WKL.AS",
    ]
    ibex = [
        "ACS.MC","ACX.MC","AMS.MC","ANA.MC","BBVA.MC","BKT.MC","CABK.MC","CLNX.MC",
        "COL.MC","ELE.MC","ENG.MC","FDR.MC","FER.MC","GRF.MC","IAG.MC","IBE.MC",
        "IDR.MC","ITX.MC","LOG.MC","MAP.MC","MEL.MC","MRL.MC","MTS.MC","NTGY.MC",
        "RED.MC","REE.MC","REP.MC","ROVI.MC","SAB.MC","SAN.MC","SGRE.MC","SOL.MC",
        "TEF.MC","UNI.MC","VIS.MC",
    ]
    smi_idx = [
        "ABBN.SW","ADEN.SW","ALC.SW","CSGN.SW","GEBN.SW","GIVN.SW","CFR.SW",
        "HOLN.SW","LONN.SW","NESN.SW","NOVN.SW","ROG.SW","SANN.SW","SCMN.SW",
        "SGSN.SW","SLHN.SW","SRENH.SW","UBSG.SW","ZURN.SW",
    ]
    mib = [
        "A2A.MI","AMP.MI","ATL.MI","AZM.MI","BMED.MI","BMPS.MI","BZU.MI","CPR.MI",
        "DIA.MI","ENEL.MI","ENI.MI","EXOR.MI","FCA.MI","FBK.MI","G.MI","HER.MI",
        "ISP.MI","IVG.MI","LDO.MI","MB.MI","MONC.MI","PIRC.MI","PRY.MI","PST.MI",
        "REC.MI","SRG.MI","STM.MI","TEN.MI","TIT.MI","TRN.MI","UCG.MI","UNI.MI",
    ]
    omx = [
        "ABB.ST","ALFA.ST","ASSA-B.ST","AZN.ST","ATCO-A.ST","BOL.ST","ERIC-B.ST",
        "ESSITY-B.ST","EVO.ST","GETI-B.ST","HEXA-B.ST","HM-B.ST","HUFV-A.ST",
        "INVE-B.ST","KINV-B.ST","NDA-SE.ST","SAND.ST","SCA-B.ST","SEB-A.ST",
        "SECU-B.ST","SKA-B.ST","SKF-B.ST","SSAB-A.ST","SHB-A.ST","SWED-A.ST",
        "SWMA.ST","TEL2-B.ST","TELIA.ST","VOLV-B.ST","VOLCAR-B.ST",
    ]
    obx = [
        "AKERBP.OL","AKSO.OL","AKER.OL","AMSC.OL","AUTO.OL","BAKKA.OL","DNB.OL",
        "EQNR.OL","FRO.OL","GOGL.OL","MOWI.OL","NEL.OL","NHY.OL","NSKOG.OL",
        "ORK.OL","PGS.OL","REC.OL","SALM.OL","SCHA.OL","SDRL.OL","SNOG.OL",
        "STB.OL","SUBC.OL","TEL.OL","TOM.OL","TGS.OL","VAR.OL","WILS.OL","YAR.OL",
    ]
    bel = [
        "ABI.BR","ACKB.BR","AGS.BR","APAM.BR","ARGX.BR","COLR.BR","D5MT.BR",
        "EKTA-B.BR","GBL.BR","GLPG.BR","KBC.BR","MELE.BR","ONTEX.BR","PROX.BR",
        "SOLB.BR","TNET.BR","UCB.BR","UMI.BR","WDP.BR",
    ]
    wig = [
        "ALE.WA","CCC.WA","CDR.WA","CPS.WA","DNP.WA","JSW.WA","KGH.WA","KRU.WA",
        "LPP.WA","MBK.WA","OPL.WA","PCO.WA","PEO.WA","PGE.WA","PKN.WA","PKO.WA",
        "PZU.WA","SPL.WA","TPE.WA","XTB.WA",
    ]
    all_eu = list(set(dax+cac+ftse+aex+ibex+smi_idx+mib+omx+obx+bel+wig))
    print(f"  EU: {len(all_eu)} tickerow")
    return all_eu

# ══════════════════════════════════════════════════════════════
#  SMI
# ══════════════════════════════════════════════════════════════

def ema(series, length):
    return series.ewm(span=length, adjust=False).mean()

def ema_ema(series, length):
    return ema(ema(series, length), length)

def calc_smi(high, low, close, lk=10, ld=3, le=3):
    hh    = high.rolling(lk).max()
    ll    = low.rolling(lk).min()
    hlr   = hh - ll
    rr    = close - (hh + ll) / 2
    denom = ema_ema(hlr, ld).replace(0, np.nan)
    smi     = 200 * (ema_ema(rr, ld) / denom)
    smi_ema = ema(smi, le)
    return smi, smi_ema

def detect_bullish_divergence(close, smi, lookback=14):
    if len(close) < lookback + 5 or len(smi) < lookback + 5:
        return False, ""
    c = close.values[-lookback:]
    s = smi.values[-lookback:]
    price_lows = []
    for i in range(2, len(c) - 2):
        if c[i] < c[i-1] and c[i] < c[i+1] and c[i] < c[i-2] and c[i] < c[i+2]:
            price_lows.append((i, float(c[i]), float(s[i])))
    if len(price_lows) < 2:
        return False, ""
    _, p1_price, p1_smi = price_lows[-2]
    _, p2_price, p2_smi = price_lows[-1]
    if p2_price < p1_price and p2_smi > p1_smi:
        dp = round((p2_price - p1_price) / p1_price * 100, 1)
        ds = round(p2_smi - p1_smi, 1)
        return True, f"div_bull (cena {dp}%, SMI +{ds})"
    return False, ""

def smi_weekly_signal(smi, smi_ema):
    if len(smi) < 4:
        return None, None, None, "--"
    s0=float(smi.iloc[-1]); s1=float(smi.iloc[-2])
    s2=float(smi.iloc[-3]); e0=float(smi_ema.iloc[-1]); e1=float(smi_ema.iloc[-2])
    if any(np.isnan(v) for v in [s0,s1,s2,e0,e1]):
        return None, None, None, "--"
    if   s0 >= 40:  zone = "OVERBOUGHT"
    elif s0 <= -40: zone = "OVERSOLD"
    elif s0 > 0:    zone = "Bullish"
    else:           zone = "Bearish"
    cross_up = (s1 < e1) and (s0 >= e0)
    exit_os  = (s1 < -40) and (s0 >= -40)
    if cross_up or exit_os:
        strong = (s1 <= -40) or (s0 <= -40)
        return ("Strong BUY" if strong else "BUY"), round(s0,2), round(e0,2), zone
    if (s2 >= s1) and (s0 > s1) and (s0 < e0):
        return "Turning Up", round(s0,2), round(e0,2), zone
    return None, round(s0,2), round(e0,2), zone


def _smi_cross_mask(smi, smi_ema):
    """
    Zwraca boolowską maskę (pandas Series) wskazującą bary, w których
    nastąpił crossover SMI w górę (cross_up lub exit_os) – ten sam
    warunek co w smi_weekly_signal, ale liczony dla całej serii naraz.
    Używane do przeszukiwania historii D1 wstecz w poszukiwaniu daty sygnału.
    """
    s  = smi
    e  = smi_ema
    s_prev = s.shift(1)
    e_prev = e.shift(1)
    cross_up = (s_prev < e_prev) & (s >= e)
    exit_os  = (s_prev < -40) & (s >= -40)
    return (cross_up | exit_os).fillna(False)


def calc_d1_lead(df_daily, w1_signal_zone, lookback_days=60):
    """
    Liczy SMI(10,3,3) na danych dziennych i szuka najnowszego crossovera
    w oknie `lookback_days` wstecz. Zwraca dict z informacją o wyprzedzeniu
    sygnału dziennego względem sygnału tygodniowego.

    Zwraca:
      d1_signal_now   – czy SMI D1 sygnalizuje crossover w *ostatniej* sesji
      d1_zone_now     – aktualna strefa SMI na D1
      d1_last_cross_days_ago – ile sesji D1 temu był ostatni crossover (None jeśli brak w oknie)
      early_signal    – True gdy D1 sygnalizuje teraz, a W1 jeszcze nie (zone W1 nie jest OVERSOLD/Bullish z crossover)
    """
    out = {
        "d1_signal_now": False,
        "d1_zone_now": "--",
        "d1_last_cross_days_ago": None,
        "early_signal": False,
    }
    req = {"High", "Low", "Close"}
    if df_daily is None or len(df_daily) < SMI_LEN_K + 10 or not req.issubset(df_daily.columns):
        return out

    try:
        smi_d, smi_d_ema = calc_smi(df_daily["High"], df_daily["Low"], df_daily["Close"],
                                     SMI_LEN_K, SMI_LEN_D, SMI_LEN_EMA)
        if len(smi_d) < 4:
            return out

        s0 = float(smi_d.iloc[-1])
        if not np.isnan(s0):
            if   s0 >= 40:  out["d1_zone_now"] = "OVERBOUGHT"
            elif s0 <= -40: out["d1_zone_now"] = "OVERSOLD"
            elif s0 > 0:    out["d1_zone_now"] = "Bullish"
            else:           out["d1_zone_now"] = "Bearish"

        mask = _smi_cross_mask(smi_d, smi_d_ema)
        recent = mask.iloc[-lookback_days:] if len(mask) > lookback_days else mask
        out["d1_signal_now"] = bool(recent.iloc[-1]) if len(recent) else False

        true_idx = recent[recent].index
        if len(true_idx) > 0:
            last_cross_pos_in_recent = recent.index.get_loc(true_idx[-1])
            days_ago = len(recent) - 1 - last_cross_pos_in_recent
            out["d1_last_cross_days_ago"] = int(days_ago)

        # Early Signal: D1 sygnalizuje teraz, ale strefa W1 nadal Bearish/OVERBOUGHT
        # (czyli W1 jeszcze nie potwierdził tego samego ruchu)
        if out["d1_signal_now"] and w1_signal_zone not in ("OVERSOLD", "Bullish"):
            out["early_signal"] = True

    except Exception:
        pass

    return out


def calc_monthly_zone(df_monthly):
    """
    Liczy SMI(10,3,3) na danych miesięcznych i zwraca strefę + czy trend
    jest zgodny z kierunkiem byczym (SMI >= EMA, czyli momentum rosnące).
    Monthly służy jako filtr głównego trendu – nie blokuje sygnałów,
    tylko ostrzega gdy W1 sugeruje wejście wbrew trendowi nadrzędnemu.

    Zwraca dict:
      m_zone        – strefa SMI miesięcznego ("--" gdy brak danych)
      m_bullish     – bool, SMI >= EMA na Monthly (momentum rosnące)
      m_risk_warning– bool, Monthly wyraźnie Bearish mimo sygnału W1
    """
    out = {"m_zone": "--", "m_bullish": False, "m_risk_warning": False}
    req = {"High", "Low", "Close"}
    if df_monthly is None or len(df_monthly) < SMI_LEN_K + 4 or not req.issubset(df_monthly.columns):
        return out
    try:
        smi_m, smi_m_ema = calc_smi(df_monthly["High"], df_monthly["Low"], df_monthly["Close"],
                                     SMI_LEN_K, SMI_LEN_D, SMI_LEN_EMA)
        if len(smi_m) < 2:
            return out
        s0 = float(smi_m.iloc[-1])
        e0 = float(smi_m_ema.iloc[-1])
        if np.isnan(s0) or np.isnan(e0):
            return out
        if   s0 >= 40:  out["m_zone"] = "OVERBOUGHT"
        elif s0 <= -40: out["m_zone"] = "OVERSOLD"
        elif s0 > 0:    out["m_zone"] = "Bullish"
        else:           out["m_zone"] = "Bearish"
        out["m_bullish"] = s0 >= e0
        # Ryzyko: Monthly wyraźnie Bearish (SMI poniżej EMA i w strefie ujemnej)
        out["m_risk_warning"] = (not out["m_bullish"]) and out["m_zone"] in ("Bearish", "OVERSOLD") and s0 < -10
    except Exception:
        pass
    return out


def calc_mtf_score(r):
    """
    Multi-TimeFrame Score (0-5) – łączy zgodność trendu D1, W1, Monthly
    w jedną liczbę. Wymaga, by w `r` znajdowały się już pola:
      smi, smi_ema (W1), d1_zone_now, d1_signal_now, m_zone, m_bullish.

    Punktacja:
      +1  SMI(D1) w strefie Bullish/OVERSOLD-wychodzącej (momentum dzienne rosnące)
      +1  SMI(W1) >= EMA(W1)  (tydzień potwierdza kierunek)
      +1  SMI(Monthly) >= EMA(Monthly)  (miesiąc potwierdza kierunek)
      +1  bonus: strefa W1 == OVERSOLD (wejście z dyskontem, nie pościg)
      +1  bonus: D1 i Monthly zgodne kierunkowo (oba bycze lub oba w OVERSOLD wychodzącym)
    """
    score = 0

    d1_zone = r.get("d1_zone_now", "--")
    d1_ok   = d1_zone in ("Bullish", "OVERSOLD")
    if d1_ok:
        score += 1

    w1_smi  = r.get("smi")
    w1_ema  = r.get("smi_ema")
    w1_ok   = (w1_smi is not None and w1_ema is not None and w1_smi >= w1_ema)
    if w1_ok:
        score += 1

    m_bullish = r.get("m_bullish", False)
    if m_bullish:
        score += 1

    if r.get("zone") == "OVERSOLD":
        score += 1

    m_zone = r.get("m_zone", "--")
    if d1_ok and m_zone in ("Bullish", "OVERSOLD"):
        score += 1

    return min(score, MTF_SCORE_MAX)



def _download_batch(batch, period, interval):
    """Pobiera jeden batch tickerów. Używane przez bulk_download w puli wątków."""
    try:
        if len(batch) == 1:
            raw = yf.download(batch[0], period=period, interval=interval,
                              auto_adjust=True, progress=False)
            if raw is not None and not raw.empty:
                return {batch[0]: raw}
            return {}
        raw = yf.download(batch, period=period, interval=interval,
                          group_by="ticker", auto_adjust=True,
                          progress=False, threads=True)
        if raw is None or raw.empty:
            return {}
        out = {}
        for ticker in batch:
            try:
                df = raw[ticker].dropna(how="all")
                if not df.empty and len(df) >= SMI_LEN_K + 5:
                    out[ticker] = df
            except Exception:
                pass
        return out
    except Exception:
        return {}


def bulk_download(tickers, period, interval):
    result  = {}
    batches = [tickers[i:i+DOWNLOAD_BATCH_SIZE]
               for i in range(0, len(tickers), DOWNLOAD_BATCH_SIZE)]
    # OPT 3: batche pobierane równolegle (4 wątki = 4 połączenia do YF jednocześnie)
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_download_batch, b, period, interval): b
                   for b in batches if b}
        for future in as_completed(futures):
            result.update(future.result())
    return result

# ══════════════════════════════════════════════════════════════
#  FAZA 1 – Tygodniowe sygnały SMI + Wyckoff
# ══════════════════════════════════════════════════════════════

def phase1_weekly_signals(ticker_market_list):
    tickers    = [t for t, _ in ticker_market_list]
    market_map = {t: m for t, m in ticker_market_list}
    print(f"\n[1/2] Dane tygodniowe -- {len(tickers)} tickerow...")
    data = bulk_download(tickers, period="2y", interval="1wk")
    print(f"      Pobrano: {len(data)}")

    index_returns = {}
    for mkt, idx_ticker in [("USA", MARKET_DIRECTION_USA), ("EU", MARKET_DIRECTION_EU)]:
        try:
            df_idx = yf.download(idx_ticker, period="2y", interval="1wk",
                                 auto_adjust=True, progress=False)
            if df_idx is not None and not df_idx.empty and len(df_idx) >= 52:
                c = df_idx["Close"].dropna()
                index_returns[mkt] = float(c.iloc[-1])/float(c.iloc[-53])-1 if len(c)>=53 else None
        except Exception:
            index_returns[mkt] = None

    signals = {}
    for ticker, df in data.items():
        try:
            smi, smi_e = calc_smi(df["High"], df["Low"], df["Close"],
                                  SMI_LEN_K, SMI_LEN_D, SMI_LEN_EMA)
            sig, s_val, e_val, zone = smi_weekly_signal(smi, smi_e)
            if sig is None:
                continue   # OPT 4: Wyckoff liczymy tylko dla spółek z sygnałem

            div_bull, div_desc = detect_bullish_divergence(df["Close"], smi)

            vol_confirm = False
            try:
                if "Volume" in df.columns:
                    vs = df["Volume"].dropna()
                    if len(vs) >= 52:
                        avg50 = float(vs.iloc[-51:-1].mean())
                        cur   = float(vs.iloc[-1])
                        vol_confirm = avg50 > 0 and cur >= avg50 * VOL_CONFIRM_MULT
            except Exception:
                pass

            rs_12m = None
            try:
                cl = df["Close"].dropna()
                if len(cl) >= 53:
                    sr = float(cl.iloc[-1]) / float(cl.iloc[-53]) - 1
                    ir = index_returns.get(market_map[ticker])
                    if ir is not None and ir != -1:
                        rs_12m = round((1 + sr) / (1 + ir), 4)
            except Exception:
                pass

            # OPT 4: Wyckoff tylko dla tickerów z sygnałem SMI (~12% universum)
            wyk = wyckoff_score(df)

            signals[ticker] = {
                "market":          market_map[ticker],
                "smi": s_val,      "smi_ema": e_val,
                "zone": zone,      "signal": sig,
                "divergence_bull": div_bull,
                "divergence_desc": div_desc,
                "vol_confirm":     vol_confirm,
                "rs_12m":          rs_12m,
                "wyckoff_score":   wyk["score"],
                "wyckoff_phase":   wyk["phase"],
                "wyckoff_dist":    wyk["dist_warning"],
                "wyckoff_dsig":    wyk["dist_signals"],
            }
        except Exception:
            pass

    s  = sum(1 for v in signals.values() if v["signal"] == "Strong BUY")
    b  = sum(1 for v in signals.values() if v["signal"] == "BUY")
    tu = sum(1 for v in signals.values() if v["signal"] == "Turning Up")
    vc = sum(1 for v in signals.values() if v.get("vol_confirm"))
    w4 = sum(1 for v in signals.values() if v.get("wyckoff_score",0) >= 4)
    wd = sum(1 for v in signals.values() if v.get("wyckoff_dist"))
    print(f"      Sygnaly: {len(signals)} (Strong:{s} BUY:{b} Turn:{tu} "
          f"VolOK:{vc} Wyckoff>=4:{w4} Dist!:{wd})")

    # ── D1 LEAD: dla spółek z sygnałem W1 sprawdzamy wyprzedzenie SMI dziennego ──
    if signals:
        print(f"\n[1b/2] SMI dzienny (D1) -- {len(signals)} tickerow z sygnalem W1...")
        d1_data = bulk_download(list(signals.keys()), period="6mo", interval="1d")
        print(f"      Pobrano D1: {len(d1_data)}")

        early_count = 0
        lead_days_sum, lead_days_n = 0, 0
        for ticker, sig_data in signals.items():
            df_d = d1_data.get(ticker)
            lead = calc_d1_lead(df_d, sig_data["zone"]) if df_d is not None else {
                "d1_signal_now": False, "d1_zone_now": "--",
                "d1_last_cross_days_ago": None, "early_signal": False,
            }
            sig_data["d1_signal_now"]          = lead["d1_signal_now"]
            sig_data["d1_zone_now"]            = lead["d1_zone_now"]
            sig_data["d1_last_cross_days_ago"] = lead["d1_last_cross_days_ago"]
            sig_data["early_signal"]           = lead["early_signal"]
            if lead["early_signal"]:
                early_count += 1
            if lead["d1_last_cross_days_ago"] is not None:
                lead_days_sum += lead["d1_last_cross_days_ago"]
                lead_days_n   += 1

        avg_lead = round(lead_days_sum / lead_days_n, 1) if lead_days_n else None
        print(f"      D1 Early Signal: {early_count} spolek | "
              f"Srednie wyprzedzenie D1: {avg_lead if avg_lead is not None else '--'} sesji")

    return signals

# ══════════════════════════════════════════════════════════════
#  FAZA 2 – Dane meta + fundamenty  (z cache FIX 5)
# ══════════════════════════════════════════════════════════════

def _calc_roic(tkr):
    try:
        fin = tkr.financials; bs = tkr.balance_sheet
        if fin is None or bs is None or fin.empty or bs.empty: return None
        ebit = None
        for label in ["Operating Income","EBIT","Earnings Before Interest And Taxes"]:
            if label in fin.index:
                v = fin.loc[label].dropna()
                if len(v) >= 1: ebit = float(v.iloc[0])
                break
        if ebit is None: return None
        tax_rate = 0.21
        try:
            tp, pt = None, None
            for l in ["Tax Provision","Income Tax Expense"]:
                if l in fin.index:
                    v = fin.loc[l].dropna()
                    if len(v) >= 1: tp = float(v.iloc[0]); break
            for l in ["Pretax Income","Income Before Tax"]:
                if l in fin.index:
                    v = fin.loc[l].dropna()
                    if len(v) >= 1: pt = float(v.iloc[0]); break
            if tp and pt and pt != 0:
                tax_rate = max(0.0, min(0.5, tp/pt))
        except Exception: pass
        nopat = ebit * (1 - tax_rate)
        ta, cl, cash = None, 0.0, 0.0
        for l in ["Total Assets"]:
            if l in bs.index:
                v = bs.loc[l].dropna()
                if len(v) >= 1: ta = float(v.iloc[0]); break
        for l in ["Current Liabilities","Total Current Liabilities"]:
            if l in bs.index:
                v = bs.loc[l].dropna()
                if len(v) >= 1: cl = float(v.iloc[0]); break
        for l in ["Cash And Cash Equivalents","Cash","Cash Cash Equivalents And Short Term Investments"]:
            if l in bs.index:
                v = bs.loc[l].dropna()
                if len(v) >= 1: cash = float(v.iloc[0]); break
        if ta is None or ta == 0: return None
        ic = ta - cl - cash
        return round(nopat/ic, 4) if ic > 0 else None
    except Exception: return None

def _calc_debt_equity(tkr):
    try:
        bs = tkr.balance_sheet
        if bs is None or bs.empty: return None
        debt, equity = None, None
        for l in ["Total Debt","Long Term Debt"]:
            if l in bs.index:
                v = bs.loc[l].dropna()
                if len(v) >= 1: debt = float(v.iloc[0]); break
        if debt is None:
            ltd, std = 0.0, 0.0
            for l in ["Long Term Debt","Long Term Debt And Capital Lease Obligation"]:
                if l in bs.index:
                    v = bs.loc[l].dropna()
                    if len(v) >= 1: ltd = float(v.iloc[0]); break
            for l in ["Current Debt","Short Term Debt","Short Long Term Debt"]:
                if l in bs.index:
                    v = bs.loc[l].dropna()
                    if len(v) >= 1: std = float(v.iloc[0]); break
            debt = ltd + std
        for l in ["Stockholders Equity","Total Stockholders Equity",
                  "Common Stock Equity","Total Equity Gross Minority Interest"]:
            if l in bs.index:
                v = bs.loc[l].dropna()
                if len(v) >= 1: equity = float(v.iloc[0]); break
        if equity is None or equity <= 0 or debt is None: return None
        return round(debt/equity, 3)
    except Exception: return None

def _calc_eps_growth(tkr):
    """
    Oblicza dynamikę EPS z danych kwartalnych Yahoo Finance.
    Zwraca dict z kluczami:
      eps_q0      – EPS ostatniego kwartału
      eps_q1      – EPS poprzedniego kwartału  (QoQ)
      eps_q4      – EPS tego samego kwartału rok temu (YoY)
      eps_qoq     – wzrost QoQ w % (None jeśli niemożliwy do obliczenia)
      eps_yoy     – wzrost YoY w % (None jeśli niemożliwy do obliczenia)
    Wszystkie wartości None gdy dane niedostępne.
    """
    empty = {"eps_q0": None, "eps_q1": None, "eps_q4": None,
             "eps_qoq": None, "eps_yoy": None}
    try:
        qfin = tkr.quarterly_financials
        if qfin is None or qfin.empty:
            return empty

        # Szukamy wiersza z EPS (diluted lub basic)
        eps_row = None
        for label in ["Diluted EPS", "Basic EPS",
                      "Diluted Normalized EPS", "Basic Normalized EPS"]:
            if label in qfin.index:
                eps_row = qfin.loc[label].dropna()
                break

        # Fallback: Net Income / Diluted Average Shares
        if eps_row is None or len(eps_row) < 2:
            ni_row  = None
            shr_row = None
            for l in ["Net Income", "Net Income Common Stockholders"]:
                if l in qfin.index:
                    ni_row = qfin.loc[l].dropna(); break
            for l in ["Diluted Average Shares", "Average Dilution Earnings"]:
                if l in qfin.index:
                    shr_row = qfin.loc[l].dropna(); break
            if ni_row is not None and shr_row is not None and len(ni_row) >= 2:
                common = ni_row.index.intersection(shr_row.index)
                if len(common) >= 2:
                    ni  = ni_row[common]
                    shr = shr_row[common].replace(0, float("nan"))
                    eps_row = (ni / shr).dropna()

        if eps_row is None or len(eps_row) < 2:
            return empty

        # Kolumny są posortowane od najnowszego do najstarszego
        eps_row = eps_row.sort_index(ascending=False)
        vals    = [float(v) for v in eps_row.values]

        q0 = vals[0] if len(vals) > 0 else None   # ostatni kwartał
        q1 = vals[1] if len(vals) > 1 else None   # poprzedni kwartał
        q4 = vals[4] if len(vals) > 4 else None   # ten sam kwartał rok temu

        def _pct(new, old):
            if new is None or old is None: return None
            if old == 0:
                return None   # dzielenie przez 0 – brak sensu
            return round((new - old) / abs(old) * 100, 1)

        return {
            "eps_q0":  round(q0, 4) if q0 is not None else None,
            "eps_q1":  round(q1, 4) if q1 is not None else None,
            "eps_q4":  round(q4, 4) if q4 is not None else None,
            "eps_qoq": _pct(q0, q1),
            "eps_yoy": _pct(q0, q4),
        }
    except Exception:
        return empty


def _calc_gross_margin(tkr):
    try:
        fin = tkr.financials
        if fin is None or fin.empty: return None
        gp, rev = None, None
        if "Gross Profit" in fin.index:
            v = fin.loc["Gross Profit"].dropna()
            if len(v) >= 1: gp = float(v.iloc[0])
        for l in ["Total Revenue","Operating Revenue"]:
            if l in fin.index:
                v = fin.loc[l].dropna()
                if len(v) >= 1: rev = float(v.iloc[0]); break
        if gp is None or rev is None or rev == 0: return None
        return round(gp/rev, 4)
    except Exception: return None

def _fetch_fundamentals(symbol: str) -> dict | None:
    """
    Pobiera surowe dane fundamentalne z Yahoo Finance dla jednego symbolu.
    Zwraca dict gotowy do złożenia z danymi tygodniowymi lub None przy błędzie.
    Nie zawiera pól z weekly_data – te są dokładane w _collect_one.
    """
    try:
        tkr = yf.Ticker(symbol)
        fi  = tkr.fast_info
        price    = getattr(fi, "last_price", None)
        cap      = getattr(fi, "market_cap", None)
        vol      = getattr(fi, "three_month_average_volume", None) or getattr(fi, "last_volume", None)
        currency = getattr(fi, "currency", "USD")
        high_52w = getattr(fi, "year_high", None)
        if not cap or cap < MIN_MARKET_CAP: return None
        if not vol or vol < MIN_VOLUME:     return None
        discount_pct = None
        if high_52w and price and high_52w > 0:
            discount_pct = round((high_52w - price) / high_52w * 100, 1)
        name=symbol; sector="--"; country="--"; eps_ttm=None; sales=None; qr=None
        try:
            info = tkr.info
            if info and len(info) > 5:
                name    = info.get("shortName") or symbol
                sector  = info.get("sector")    or "--"
                country = info.get("country")   or "--"
                eps     = info.get("trailingEps")
                if eps is not None: eps_ttm = round(float(eps), 2)
        except Exception: pass
        try:
            fin = tkr.financials; bs = tkr.balance_sheet
        except Exception:
            fin, bs = None, None
        try:
            if fin is not None and not fin.empty:
                for l in ["Total Revenue","Operating Revenue"]:
                    if l in fin.index:
                        rv = fin.loc[l].dropna()
                        if len(rv) >= 1: sales = round(float(rv.iloc[0])/1e6,1); break
        except Exception: pass
        try:
            if bs is not None and not bs.empty:
                ca, inv, cl = None, 0.0, None
                for l in ["Current Assets","Total Current Assets"]:
                    if l in bs.index:
                        v = bs.loc[l].dropna()
                        if len(v) >= 1: ca = float(v.iloc[0]); break
                for l in ["Inventory","Inventories"]:
                    if l in bs.index:
                        v = bs.loc[l].dropna()
                        if len(v) >= 1: inv = float(v.iloc[0]); break
                for l in ["Current Liabilities","Total Current Liabilities"]:
                    if l in bs.index:
                        v = bs.loc[l].dropna()
                        if len(v) >= 1: cl = float(v.iloc[0]); break
                if ca is not None and cl is not None and cl > 0:
                    qr = round((ca-inv)/cl, 2)
        except Exception: pass
        roic        = _calc_roic(tkr)
        debt_equity = _calc_debt_equity(tkr)
        gross_margin= _calc_gross_margin(tkr)
        eps_growth  = _calc_eps_growth(tkr)
        return {
            "ticker": symbol,
            "name": name, "country": country, "sector": sector,
            "price": round(price,2) if price else None, "currency": currency,
            "high_52w": round(high_52w,2) if high_52w else None,
            "discount_52w": discount_pct,
            "market_cap_mln": round(cap/1e6,1),
            "volume_k": round(vol/1000,1),
            "eps_ttm": eps_ttm, "sales_ttm_mln": sales, "quick_ratio": qr,
            "roic": roic, "debt_equity": debt_equity, "gross_margin": gross_margin,
            "eps_q0":  eps_growth["eps_q0"],
            "eps_q1":  eps_growth["eps_q1"],
            "eps_q4":  eps_growth["eps_q4"],
            "eps_qoq": eps_growth["eps_qoq"],
            "eps_yoy": eps_growth["eps_yoy"],
            "scanned_at": datetime.now().isoformat(),
        }
    except Exception:
        return None

def _collect_one(symbol: str, weekly_data: dict) -> dict | None:
    """
    Łączy dane fundamentalne (z cache lub świeżo pobrane) z danymi tygodniowymi.
    FIX 5: fundamenty trafiają do cache na 24h – kolejne uruchomienia w tym oknie
    pomijają wywołania do Yahoo Finance dla spółek bez zmiany sygnału.
    """
    # Próba odczytu z cache
    cached = _load_cache(symbol)
    if cached is not None:
        fund = cached
    else:
        fund = _fetch_fundamentals(symbol)
        if fund is None:
            return None
        _save_cache(symbol, fund)

    # Scalamy fundamenty z danymi tygodniowymi (sygnał SMI + Wyckoff)
    return {
        **fund,
        "market":        weekly_data["market"],
        "smi":           weekly_data["smi"],
        "smi_ema":       weekly_data["smi_ema"],
        "zone":          weekly_data["zone"],
        "signal":        weekly_data["signal"],
        "divergence_bull": weekly_data.get("divergence_bull", False),
        "divergence_desc": weekly_data.get("divergence_desc", ""),
        "vol_confirm":   weekly_data.get("vol_confirm", False),
        "rs_12m":        weekly_data.get("rs_12m", None),
        "wyckoff_score": weekly_data.get("wyckoff_score", 0),
        "wyckoff_phase": weekly_data.get("wyckoff_phase", "–"),
        "wyckoff_dist":  weekly_data.get("wyckoff_dist",  False),
        "wyckoff_dsig":  weekly_data.get("wyckoff_dsig",  []),
        "d1_signal_now":          weekly_data.get("d1_signal_now", False),
        "d1_zone_now":            weekly_data.get("d1_zone_now", "--"),
        "d1_last_cross_days_ago": weekly_data.get("d1_last_cross_days_ago", None),
        "early_signal":           weekly_data.get("early_signal", False),
    }

def phase2_collect(weekly_signals):
    if not weekly_signals: return []
    candidates = list(weekly_signals.keys())
    print(f"\n[2/2] Meta + fundamenty -- {len(candidates)} tickerow "
          f"({FUNDAMENTALS_WORKERS} watkow)...")

    # Statystyki cache
    cache_hits = sum(1 for s in candidates if _load_cache(s) is not None)
    print(f"      Cache: {cache_hits} trafien / {len(candidates)} tickerow")

    results = []
    with ThreadPoolExecutor(max_workers=FUNDAMENTALS_WORKERS) as pool:
        futures = {pool.submit(_collect_one, sym, weekly_signals[sym]): sym
                   for sym in candidates}
        for future in as_completed(futures):
            r = future.result()
            if r: results.append(r)
    print(f"      Zebrano danych: {len(results)}")
    for r in results:
        r["tech_score"] = calc_tech_score(r)
    results.sort(key=lambda x: x.get("tech_score",0), reverse=True)
    return results

# ══════════════════════════════════════════════════════════════
#  FILTRY  (FIX 2 + FIX 6)
# ══════════════════════════════════════════════════════════════

def filter_main(r):
    """
    Filtr screener głównego.
    Zasada: brak danych (None) = przepuszczamy, tylko jawnie zła wartość = odrzucamy.
    FIX 6: eps <= 0 odrzucany (break-even nie liczy się jako zysk).
    """
    if r["signal"] not in ("Strong BUY", "Turning Up"):
        return False

    # Cena – opcjonalna górna granica
    price = r.get("price")
    if price is not None and price > MAX_PRICE:
        return False

    # FIX 6 – EPS: jeśli dostępne, musi być dodatnie (>0, nie >=0)
    eps = r.get("eps_ttm")
    if eps is not None and eps <= 0:
        return False

    # Quick Ratio: jeśli dostępne, musi spełniać próg
    qr = r.get("quick_ratio")
    if qr is not None and qr < MIN_QUICK:
        return False

    # Dyskonto: jeśli dostępne, musi spełniać próg
    disc = r.get("discount_52w")
    if disc is not None and disc < MIN_DISCOUNT_52W * 100:
        return False

    # ROIC: jeśli dostępne, musi spełniać próg
    roic = r.get("roic")
    if roic is not None and roic < MIN_ROIC:
        return False

    # Debt/Equity: jeśli dostępne, musi być poniżej progu
    de = r.get("debt_equity")
    if de is not None and de > MAX_DEBT_EQUITY:
        return False

    # Gross Margin: jeśli dostępne, musi spełniać próg
    gm = r.get("gross_margin")
    if gm is not None and gm < MIN_GROSS_MARGIN:
        return False

    return True

# ══════════════════════════════════════════════════════════════
#  SCORING TECHNICZNY  (FIX 4 – ważony)
# ══════════════════════════════════════════════════════════════

def calc_tech_score(sig: dict) -> int:
    """
    Ważony scoring techniczny oparty na SCORE_WEIGHTS.
    FIX 4:
      – Każdy składnik ma wagę proporcjonalną do wartości predykcyjnej.
      – Kara wyckoff_dist podniesiona z -1 do -3 (dystrybucja = silny negatyw).
      – Max nadal klipowany do 12 dla wstecznej kompatybilności wyświetlania.
    EPS Growth (propozycja 6):
      – +1 za EPS QoQ >= +15% (wzrost zysku, nie tylko jego obecność).
      – Próg 15% celowo wyższy niż sam EPS>0, żeby nagradzać rzeczywistą akcelerację.
    """
    score = 0
    W = SCORE_WEIGHTS

    # Sygnał bazowy
    sig_type = sig.get("signal", "")
    if sig_type == "Strong BUY":
        score += W["strong_buy"]
    elif sig_type == "BUY":
        score += W["buy"]

    # Strefa SMI
    zone = sig.get("zone", "")
    if zone == "OVERSOLD":
        score += W["oversold"]
    elif zone == "Bearish":
        score += W["bearish"]

    # Potwierdzenia jakościowe
    if sig.get("divergence_bull"):
        score += W["divergence"]
    if sig.get("vol_confirm"):
        score += W["vol_confirm"]

    # Fundamenty
    if (sig.get("eps_ttm") or 0) > 0:
        score += W["eps_positive"]
    # EPS Growth: nagradzamy silną akcelerację zysku (QoQ >= +15%)
    eps_qoq = sig.get("eps_qoq")
    if eps_qoq is not None and eps_qoq >= 15:
        score += W["eps_growth_strong"]
    if (sig.get("quick_ratio") or 0) >= MIN_QUICK:
        score += W["qr_ok"]
    roic = sig.get("roic")
    if roic is not None and roic >= MIN_ROIC:
        score += W["roic"]

    # O'Neil RS
    rs = sig.get("rs_12m")
    if rs is not None and rs >= RS_MIN_OUTPERFORM:
        score += W["rs_outperform"]

    # Wyckoff
    wyk = sig.get("wyckoff_score", 0)
    if wyk >= 4 and not sig.get("wyckoff_dist"):
        score += W["wyckoff_high"]
    if sig.get("wyckoff_dist"):
        score += W["wyckoff_dist"]   # wartość ujemna (-3)

    return max(0, min(score, 12))

# ══════════════════════════════════════════════════════════════
#  FORMATOWANIE
# ══════════════════════════════════════════════════════════════

def fmt_cap(mln):
    if mln is None: return "--"
    return f"{mln/1000:.1f} B" if mln >= 1000 else f"{mln:.0f} M"

def fmt_vol(k):
    if k is None: return "--"
    return f"{k/1000:.1f}M" if k >= 1000 else f"{k:.0f}K"

def fmt_pct(v):
    return f"{v*100:.1f}%" if v is not None else "--"

def na(v, suffix=""):
    return f"{v}{suffix}" if v is not None else "--"

# ══════════════════════════════════════════════════════════════
#  WSPÓLNE ELEMENTY HTML
# ══════════════════════════════════════════════════════════════

COMMON_CSS = """
  :root {
    --bg:#0b0d1a; --bg2:#11142a; --bg3:#181c35; --border:#252840;
    --text:#d0d4e8; --muted:#555d7a; --accent:#7c9ef0;
    --green:#3ecf8e; --red:#ff4560; --orange:#ff6b00; --yellow:#ffb800;
    --purple:#c471ed;
  }
  *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
  body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
       background:var(--bg);color:var(--text);min-height:100vh}
  .page{max-width:1600px;margin:0 auto;padding:2rem}
  h1{font-size:1.5rem;font-weight:700;color:#fff;letter-spacing:-.3px}
  h2{font-size:1.05rem;font-weight:600;color:#fff;margin-bottom:1rem}
  .subtitle{font-size:.8rem;color:var(--muted);margin-top:.3rem}
  .report-nav{display:flex;gap:.6rem;margin-bottom:1.75rem;flex-wrap:wrap}
  .nav-link{background:var(--bg2);border:1px solid var(--border);border-radius:8px;
            padding:.45rem 1.1rem;font-size:.83rem;color:var(--muted);text-decoration:none;
            transition:color .15s,border-color .15s,background .15s}
  .nav-link:hover{color:#fff;border-color:var(--accent)}
  .nav-link-active{color:#fff;border-color:var(--accent);background:var(--bg3);pointer-events:none}
  .stats-bar{display:flex;flex-wrap:wrap;gap:.75rem;margin:1.5rem 0}
  .stat{background:var(--bg2);border:1px solid var(--border);border-radius:8px;
        padding:.6rem 1.1rem;min-width:110px}
  .stat-val{font-size:1.4rem;font-weight:700;color:#fff}
  .stat-val.green{color:var(--green)} .stat-val.orange{color:var(--orange)}
  .stat-val.blue{color:var(--accent)} .stat-val.purple{color:var(--purple)}
  .stat-label{font-size:.72rem;color:var(--muted);margin-top:.1rem}
  .section{background:var(--bg2);border:1px solid var(--border);border-radius:12px;
           padding:1.5rem;margin-bottom:1.5rem}
  .section-strong {border-color:#ff6b00;box-shadow:0 0 20px rgba(255,107,0,.08)}
  .section-buy    {border-color:#3ecf8e;box-shadow:0 0 20px rgba(62,207,142,.06)}
  .section-turning{border-color:#7b2ff7;box-shadow:0 0 20px rgba(196,113,237,.08)}
  .section-header{display:flex;align-items:center;gap:.6rem;margin-bottom:1.2rem}
  .section-icon{font-size:1.2rem}
  .cards-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:1rem}
  .signal-card{background:var(--bg3);border:1px solid var(--border);border-radius:10px;
               padding:1.2rem;position:relative;overflow:hidden}
  .sc-ticker{font-size:1.1rem;font-weight:700;color:#fff;letter-spacing:-.3px}
  .sc-name{font-size:.75rem;color:var(--muted);margin-top:.1rem;
           white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:200px}
  .sc-price{font-size:1.3rem;font-weight:700;color:var(--accent);margin:.7rem 0}
  .sc-row{display:flex;justify-content:space-between;font-size:.78rem;
          padding:.25rem 0;border-bottom:1px solid var(--border)}
  .sc-row span:first-child{color:var(--muted)}
  .sc-divider{height:1px;background:var(--border);margin:.5rem 0}
  .sc-stoch{display:flex;gap:.5rem;margin-top:.7rem}
  .sc-stoch-item{flex:1;background:var(--bg2);border-radius:6px;padding:.4rem .6rem;text-align:center}
  .sc-stoch-label{font-size:.65rem;color:var(--muted)}
  .sc-stoch-val{font-size:.95rem;font-weight:600;color:var(--text)}
  .sc-stoch-val.green{color:var(--green)}
  .empty{color:var(--muted);text-align:center;padding:2rem;font-size:.9rem}
  .table-wrap{overflow-x:auto}
  table{width:100%;border-collapse:collapse;font-size:.8rem}
  th{background:var(--bg3);color:var(--muted);font-weight:600;text-align:left;
     padding:.6rem 1rem;border-bottom:1px solid var(--border);white-space:nowrap}
  td{padding:.55rem 1rem;border-bottom:1px solid rgba(37,40,64,.6);vertical-align:middle}
  tr:hover td{background:rgba(255,255,255,.02)}
  .num{text-align:right;font-variant-numeric:tabular-nums}
  .name-col{max-width:160px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .smi-col{color:var(--green)}
  .ticker{font-weight:600;color:#fff;margin-right:.3rem}
  .badge-strong {background:#3d1500;color:var(--orange);font-size:.68rem;font-weight:700;
                 padding:.15rem .45rem;border-radius:3px;margin-left:.2rem}
  .badge-buy    {background:#0b2318;color:var(--green);font-size:.68rem;font-weight:700;
                 padding:.15rem .45rem;border-radius:3px;margin-left:.2rem}
  .badge-turning{background:#1a0a2e;color:var(--purple);font-size:.68rem;font-weight:700;
                 padding:.15rem .45rem;border-radius:3px;margin-left:.2rem}
  .badge-usa{background:#0d1a2e;color:var(--accent);font-size:.72rem;
             padding:.15rem .5rem;border-radius:4px;border:1px solid var(--border)}
  .badge-eu {background:#1a1a0d;color:var(--yellow);font-size:.72rem;
             padding:.15rem .5rem;border-radius:4px;border:1px solid var(--border)}
  .zone-badge{font-size:.72rem;padding:.15rem .5rem;border-radius:4px;font-weight:500}
  .zone-ob  {background:#3d0010;color:#ff4560}
  .zone-os  {background:#0b2318;color:var(--green)}
  .zone-bull{background:#0d1a2e;color:var(--accent)}
  .zone-bear{background:#1a1505;color:#ffa040}
  .badge-score-high {background:#0b2318;color:#3ecf8e;font-size:.68rem;font-weight:700;
                     padding:.15rem .45rem;border-radius:3px;margin-left:.3rem}
  .badge-score-mid  {background:#0d1a2e;color:#7c9ef0;font-size:.68rem;font-weight:700;
                     padding:.15rem .45rem;border-radius:3px;margin-left:.3rem}
  .badge-score-low  {background:#1a1505;color:#ffa040;font-size:.68rem;font-weight:700;
                     padding:.15rem .45rem;border-radius:3px;margin-left:.3rem}
  .badge-div        {background:#1a0a2e;color:#c471ed;font-size:.68rem;font-weight:700;
                     padding:.15rem .45rem;border-radius:3px;margin-left:.3rem}
  .badge-wyk-good   {background:#0a2e10;color:#3ecf8e;font-size:.65rem;font-weight:700;
                     padding:.12rem .4rem;border-radius:3px;margin-left:.3rem}
  .badge-wyk-dist   {background:#3d0a0a;color:#ff6b6b;font-size:.65rem;font-weight:700;
                     padding:.12rem .4rem;border-radius:3px;margin-left:.3rem}
  .badge-wyk-mid    {background:#0d1a2e;color:#7c9ef0;font-size:.65rem;font-weight:700;
                     padding:.12rem .4rem;border-radius:3px;margin-left:.3rem}
  .badge-d1-lead    {background:#1a2e0a;color:#9be564;font-size:.65rem;font-weight:700;
                     padding:.12rem .4rem;border-radius:3px;margin-left:.3rem}
  .badge-early      {background:#2e1a00;color:#ffb800;font-size:.68rem;font-weight:700;
                     padding:.15rem .5rem;border-radius:4px}
  .badge-mtf-high   {background:#0a2e10;color:#3ecf8e;font-size:.68rem;font-weight:700;
                     padding:.15rem .45rem;border-radius:3px;margin-left:.3rem}
  .badge-mtf-mid    {background:#0d1a2e;color:#7c9ef0;font-size:.68rem;font-weight:700;
                     padding:.15rem .45rem;border-radius:3px;margin-left:.3rem}
  .badge-mtf-low    {background:#1a1505;color:#ffa040;font-size:.68rem;font-weight:700;
                     padding:.15rem .45rem;border-radius:3px;margin-left:.3rem}
  .badge-m-risk     {background:#3d0010;color:#ff4560;font-size:.7rem;font-weight:700;
                     padding:.18rem .55rem;border-radius:4px;border:1px solid #ff456066}
  @media(max-width:900px){.page{padding:1rem} th,td{padding:.45rem .6rem}}
"""

def _signal_cfg(sig):
    if sig == "Strong BUY":
        return "linear-gradient(90deg,#ff6b00,#ffb800)", "STRONG BUY", "#ffb800"
    if sig == "BUY":
        return "linear-gradient(90deg,#1a9e5c,#3ecf8e)", "BUY", "#3ecf8e"
    return "linear-gradient(90deg,#7b2ff7,#c471ed)", "TURNING UP", "#c471ed"

def _zone_color(z):
    return {"OVERBOUGHT":"#ff4560","OVERSOLD":"#00e599",
            "Bullish":"#4da6ff","Bearish":"#ffa040"}.get(z,"#888")

def _zone_badge(zone):
    cls = {"OVERBOUGHT":"zone-ob","OVERSOLD":"zone-os",
           "Bullish":"zone-bull","Bearish":"zone-bear"}.get(zone,"")
    return f'<span class="zone-badge {cls}">{zone}</span>'

def _score_badge(score):
    if score is None: return ""
    cls = "badge-score-high" if score >= 7 else "badge-score-mid" if score >= 4 else "badge-score-low"
    return f'<span class="{cls}" title="Scoring techniczny">&#9733;{score}</span>'

def _div_badge(has_div, desc=""):
    if not has_div: return ""
    return f'<span class="badge-div" title="{desc or "Dywergencja bycza"}">DIV</span>'

def _wyk_badge(score, dist):
    if dist:
        return '<span class="badge-wyk-dist" title="Sygnał dystrybucji Wyckoffa">W:⚠Dist</span>'
    if score >= 4:
        labels = {4:"Spring", 5:"SOS"}
        return f'<span class="badge-wyk-good" title="Wyckoff faza C/D">W:{labels.get(score,score)}</span>'
    if score >= 2:
        labels = {2:"A+AR", 3:"FazaB"}
        return f'<span class="badge-wyk-mid" title="Wyckoff faza A/B">W:{labels.get(score,score)}</span>'
    return ""

def _d1_badge(days_ago, early):
    """
    Badge pokazujący wyprzedzenie SMI dziennego względem tygodniowego.
    `days_ago` to liczba sesji D1 od ostatniego crossovera D1.
    `early` oznacza że D1 sygnalizuje teraz, a W1 jeszcze nie potwierdził.
    """
    if days_ago is None:
        return ""
    if early:
        return f'<span class="badge-early" title="SMI dzienny wyprzedza tygodniowy">⚡ D1 wczesniej o {days_ago}d</span>'
    if days_ago > 0:
        return f'<span class="badge-d1-lead" title="Dni od crossover na D1">D1: -{days_ago}d</span>'
    return ""

def _mtf_badge(score):
    """Badge MTF Score (0-5) – zgodność trendu D1/W1/Monthly."""
    if score is None:
        return ""
    cls = "badge-mtf-high" if score >= 4 else "badge-mtf-mid" if score >= 2 else "badge-mtf-low"
    return f'<span class="{cls}" title="Multi-TimeFrame Score (D1+W1+Monthly)">MTF&#9670;{score}</span>'

def _monthly_risk_badge(risk_warning, m_zone):
    """
    Ostrzeżenie wizualne gdy Monthly jest wyraźnie Bearish mimo sygnału W1.
    Zwraca pusty string gdy brak ryzyka (żeby nie zaśmiecać kart bez powodu).
    """
    if not risk_warning:
        return ""
    return (f'<span class="badge-m-risk" title="SMI miesieczny w trendzie spadkowym - '
            f'sygnal W1 moze byc przedwczesny">&#9888; Monthly {m_zone}</span>')

def _color_ok(val, ok):
    if val is None: return "#888"
    return "#3ecf8e" if ok else "#ff4560"

def _eps_growth_rows(r):
    """
    Generuje 1–2 wiersze sc-row z dynamiką EPS do kart HTML.
    Wyświetla QoQ i YoY z kolorowaniem: zielony ≥0%, czerwony <0%.
    """
    qoq = r.get("eps_qoq")
    yoy = r.get("eps_yoy")
    if qoq is None and yoy is None:
        return ""

    def _fmt(val):
        if val is None:
            return '<span style="color:#555d7a">--</span>'
        color = "#3ecf8e" if val >= 0 else "#ff4560"
        sign  = "+" if val > 0 else ""
        return f'<span style="color:{color};font-weight:600">{sign}{val}%</span>'

    rows = ""
    if qoq is not None:
        rows += (f'<div class="sc-row"><span>EPS QoQ</span>{_fmt(qoq)}</div>')
    if yoy is not None:
        rows += (f'<div class="sc-row"><span>EPS YoY</span>{_fmt(yoy)}</div>')
    return rows


def _eps_growth_cells(r):
    """
    Generuje dwie komórki <td> z dynamiką EPS do tabeli HTML.
    """
    qoq = r.get("eps_qoq")
    yoy = r.get("eps_yoy")

    def _td(val):
        if val is None:
            return '<td class="num" style="color:#555d7a">--</td>'
        color = "#3ecf8e" if val >= 0 else "#ff4560"
        sign  = "+" if val > 0 else ""
        return f'<td class="num" style="color:{color};font-weight:600">{sign}{val}%</td>'

    return _td(qoq) + _td(yoy)


def render_cards(data, show_quality=False):
    if not data:
        return "<div class='empty'>Brak sygnalow</div>"
    cards = ""
    for r in sorted(data, key=lambda x: (
        {"Strong BUY":0,"BUY":1,"Turning Up":2}.get(x["signal"],9),
        -(x.get("tech_score") or 0)
    )):
        sig = r.get("signal","Strong BUY")
        tc, sl, sc = _signal_cfg(sig)
        mc  = "usa" if r["market"] == "USA" else "eu"
        z   = r.get("zone","--")
        zc  = _zone_color(z)
        wyk_s = r.get("wyckoff_score",0)
        wyk_d = r.get("wyckoff_dist", False)

        disc = r.get("discount_52w")
        disc_row = ""
        if disc is not None:
            dc = "#00e599" if disc>=50 else "#ffb800" if disc>=30 else "#888"
            disc_row = (f'<div class="sc-row"><span>Discount 52W</span>'
                        f'<span style="color:{dc};font-weight:600">-{disc}%</span></div>')

        eps=r.get("eps_ttm"); qr=r.get("quick_ratio")
        roic=r.get("roic"); de=r.get("debt_equity"); gm=r.get("gross_margin")
        vc=r.get("vol_confirm",False); rs12=r.get("rs_12m")

        quality_rows = ""
        if show_quality:
            roic_c=_color_ok(roic,roic is not None and roic>=MIN_ROIC)
            de_c  =_color_ok(de,  de   is not None and de  < MAX_DEBT_EQUITY)
            gm_c  =_color_ok(gm,  gm   is not None and gm  >=MIN_GROSS_MARGIN)
            vc_c  ="#3ecf8e" if vc else "#555d7a"
            rs12_c=_color_ok(rs12,rs12 is not None and rs12>=RS_MIN_OUTPERFORM)
            rs12_s=f"{rs12:.2f}×" if rs12 is not None else "--"
            wyk_label, wyk_bg = _wyckoff_cell(wyk_s, wyk_d)
            wyk_row = (f'<div class="sc-row"><span>Wyckoff W1</span>'
                       f'<span style="background:{wyk_bg};color:{"#ff6b6b" if wyk_d else "#3ecf8e" if wyk_s>=4 else "#7c9ef0"};'
                       f'font-weight:600;font-size:.72rem;padding:.1rem .35rem;border-radius:3px">'
                       f'{wyk_label}</span></div>')
            quality_rows = (
                f'<div class="sc-divider"></div>'
                f'<div class="sc-row"><span>ROIC</span>'
                f'<span style="color:{roic_c};font-weight:600">{fmt_pct(roic)}</span></div>'
                f'<div class="sc-row"><span>Debt/Equity</span>'
                f'<span style="color:{de_c};font-weight:600">{na(de)}</span></div>'
                f'<div class="sc-row"><span>Gross Margin</span>'
                f'<span style="color:{gm_c};font-weight:600">{fmt_pct(gm)}</span></div>'
                f'<div class="sc-divider"></div>'
                f'<div class="sc-row"><span>Vol W1</span>'
                f'<span style="color:{vc_c};font-weight:600;font-size:.72rem">{"✓ potw." if vc else "brak potw."}</span></div>'
                f'<div class="sc-row"><span>RS 12M</span>'
                f'<span style="color:{rs12_c};font-weight:600">{rs12_s}</span></div>'
                f'{wyk_row}'
            )

        monthly_risk_html = ""
        if r.get("m_risk_warning"):
            monthly_risk_html = (
                '<div style="margin:.5rem 0">'
                + _monthly_risk_badge(r.get("m_risk_warning", False), r.get("m_zone", "--"))
                + '</div>'
            )

        cards += (
            f'<div class="signal-card">'
            f'<div style="position:absolute;top:0;left:0;right:0;height:3px;background:{tc}"></div>'
            f'<div style="display:flex;justify-content:space-between;align-items:flex-start">'
            f'<div><div class="sc-ticker">{r["ticker"]}'
            f'{_score_badge(r.get("tech_score"))}'
            f'{_div_badge(r.get("divergence_bull"),r.get("divergence_desc",""))}'
            f'{_wyk_badge(wyk_s, wyk_d)}'
            f'{_d1_badge(r.get("d1_last_cross_days_ago"), r.get("early_signal", False))}'
            f'{_mtf_badge(r.get("mtf_score"))}'
            f'</div>'
            f'<div class="sc-name">{r["name"]}</div></div>'
            f'<span class="badge-{mc}">{r["market"]}</span></div>'
            f'{monthly_risk_html}'
            f'<div class="sc-price">{na(r["price"])} {r["currency"]}</div>'
            f'<div class="sc-row"><span>Sygnal</span>'
            f'<span style="color:{sc};font-weight:600">{sl}</span></div>'
            f'{disc_row}'
            f'<div class="sc-row"><span>Strefa SMI</span>'
            f'<span style="color:{zc}">{z}</span></div>'
            f'<div class="sc-row"><span>Sektor</span><span>{r["sector"]}</span></div>'
            f'<div class="sc-row"><span>Market Cap</span>'
            f'<span style="color:var(--accent)">{fmt_cap(r.get("market_cap_mln"))}</span></div>'
            f'<div class="sc-row"><span>Vol avg</span>'
            f'<span>{fmt_vol(r.get("volume_k"))}</span></div>'
            f'<div class="sc-divider"></div>'
            f'<div class="sc-row"><span>EPS TTM</span>'
            f'<span style="color:{_color_ok(r.get("eps_ttm"), (r.get("eps_ttm") or 0) > 0)}">'
            f'{na(r.get("eps_ttm"))}</span></div>'
            f'{_eps_growth_rows(r)}'
            f'<div class="sc-row"><span>Sales TTM</span>'
            f'<span>{na(r.get("sales_ttm_mln"))} M</span></div>'
            f'<div class="sc-row"><span>Quick Ratio</span>'
            f'<span style="color:{_color_ok(r.get("quick_ratio"), (r.get("quick_ratio") or 0) >= 1)}">'
            f'{na(r.get("quick_ratio"))}</span></div>'
            f'{quality_rows}'
            f'<div class="sc-stoch">'
            f'<div class="sc-stoch-item"><div class="sc-stoch-label">SMI tydz.</div>'
            f'<div class="sc-stoch-val green">{r["smi"]}</div></div>'
            f'<div class="sc-stoch-item"><div class="sc-stoch-label">SMI EMA</div>'
            f'<div class="sc-stoch-val">{r["smi_ema"]}</div></div>'
            f'</div></div>'
        )
    return f'<div class="cards-grid">{cards}</div>'

def render_table_rows(data, show_quality=False):
    if not data:
        return "<tr><td colspan='20' style='text-align:center;color:#888;padding:2rem'>Brak wynikow</td></tr>"
    data = sorted(data, key=lambda x: (
        {"Strong BUY":0,"BUY":1,"Turning Up":2}.get(x["signal"],9),
        -(x.get("tech_score") or 0)
    ))
    html = ""
    for r in data:
        disc = r.get("discount_52w")
        disc_str   = f"-{disc}%" if disc is not None else "--"
        disc_color = "#00e599" if (disc or 0)>=50 else "#ffb800" if (disc or 0)>=30 else "#888"
        sig   = r["signal"]
        badge = ('<span class="badge-strong">STRONG</span>' if sig=="Strong BUY"
                 else '<span class="badge-buy">BUY</span>' if sig=="BUY"
                 else '<span class="badge-turning">TURN</span>')
        eps=r.get("eps_ttm"); qr=r.get("quick_ratio")
        roic=r.get("roic"); de=r.get("debt_equity"); gm=r.get("gross_margin")
        vc=r.get("vol_confirm",False); rs12=r.get("rs_12m")
        wyk_s=r.get("wyckoff_score",0); wyk_d=r.get("wyckoff_dist",False)

        eps_c  = "#3ecf8e" if eps  and eps >0    else ("#ff4560" if eps  is not None else "inherit")
        qr_c   = "#3ecf8e" if qr   and qr  >=1.0 else ("#ff4560" if qr   is not None else "inherit")
        wyk_label, wyk_bg = _wyckoff_cell(wyk_s, wyk_d)
        wyk_fc = "#ff6b6b" if wyk_d else "#3ecf8e" if wyk_s>=4 else "#7c9ef0" if wyk_s>=2 else "#555d7a"

        quality_cols = ""
        if show_quality:
            roic_c=_color_ok(roic,roic is not None and roic>=MIN_ROIC)
            de_c  =_color_ok(de,  de   is not None and de < MAX_DEBT_EQUITY)
            gm_c  =_color_ok(gm,  gm   is not None and gm >=MIN_GROSS_MARGIN)
            vc_c  ="#3ecf8e" if vc else "#555d7a"
            rs12_c=_color_ok(rs12,rs12 is not None and rs12>=RS_MIN_OUTPERFORM)
            rs12_s=f"{rs12:.2f}×" if rs12 is not None else "--"
            m_zone = r.get("m_zone", "--")
            m_risk = r.get("m_risk_warning", False)
            m_c    = "#ff4560" if m_risk else "#3ecf8e" if r.get("m_bullish") else "#7c9ef0"
            quality_cols = (
                f'<td class="num" style="color:{roic_c}">{fmt_pct(roic)}</td>'
                f'<td class="num" style="color:{de_c}">{na(de)}</td>'
                f'<td class="num" style="color:{gm_c}">{fmt_pct(gm)}</td>'
                f'<td class="num" style="color:{vc_c}">{"✓" if vc else "–"}</td>'
                f'<td class="num" style="color:{rs12_c}">{rs12_s}</td>'
                f'<td class="num" style="color:{m_c};font-weight:600">{m_zone}{" ⚠" if m_risk else ""}</td>'
            )

        html += f"""<tr>
          <td><span class="ticker">{r['ticker']}</span>{badge}{_score_badge(r.get('tech_score'))}{_div_badge(r.get('divergence_bull'),r.get('divergence_desc',''))}{_d1_badge(r.get('d1_last_cross_days_ago'), r.get('early_signal', False))}{_mtf_badge(r.get('mtf_score'))}</td>
          <td class="name-col">{r['name']}</td>
          <td><span class="badge-{'usa' if r['market']=='USA' else 'eu'}">{r['market']}</span></td>
          <td>{r['sector']}</td>
          <td class="num">{na(r['price'])} {r['currency']}</td>
          <td class="num" style="color:{disc_color};font-weight:600">{disc_str}</td>
          <td class="num">{fmt_cap(r.get('market_cap_mln'))}</td>
          <td class="num">{fmt_vol(r.get('volume_k'))}</td>
          <td class="num smi-col">{r['smi']}</td>
          <td class="num">{r['smi_ema']}</td>
          <td>{_zone_badge(r['zone'])}</td>
          <td class="num" style="color:{eps_c}">{na(eps)}</td>
          {_eps_growth_cells(r)}
          <td class="num">{na(r.get('sales_ttm_mln'))} M</td>
          <td class="num" style="color:{qr_c}">{na(qr)}</td>
          <td class="num" style="background:{wyk_bg};color:{wyk_fc};font-weight:600;text-align:center">{wyk_label}</td>
          {quality_cols}
        </tr>"""
    return html

# ══════════════════════════════════════════════════════════════
#  HTML – SCREENER GŁÓWNY
# ══════════════════════════════════════════════════════════════

def generate_html_main(meta, results):
    dt = datetime.fromisoformat(meta["generated_at"]).strftime("%d.%m.%Y %H:%M")
    strong_res  = [r for r in results if r["signal"] == "Strong BUY"]
    turning_res = [r for r in results if r["signal"] == "Turning Up"]
    early_res   = [r for r in results if r.get("early_signal")]
    html = f"""<!DOCTYPE html>
<html lang="pl"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Stock Screener SMI – {dt}</title>
<style>{COMMON_CSS}
  .strategy-box{{background:var(--bg2);border:1px solid #ff6b00;border-radius:10px;
                padding:1rem 1.4rem;font-size:.82rem;line-height:1.7}}
  .strategy-box strong{{color:var(--orange)}}
  .strategy-box ul{{margin:.4rem 0 0 1.2rem;columns:2;gap:2rem}}
  @media(max-width:600px){{.strategy-box ul{{columns:1}}}}
  .section-early{{border-color:#ffb800;box-shadow:0 0 20px rgba(255,184,0,.1)}}
  .early-note{{font-size:.78rem;color:var(--muted);margin-bottom:1rem;line-height:1.6}}
</style></head><body>
<div class="page">
  <nav class="report-nav">
    <a href="index.html"     class="nav-link">&#127968; Start</a>
    <a href="screener.html"  class="nav-link nav-link-active">&#9889; Screener g&#322;&#243;wny</a>
    <a href="index_all.html" class="nav-link">&#128270; Full Scan</a>
  </nav>
  <h1>Screener g&#322;&#243;wny &mdash; SMI Tygodniowy</h1>
  <p class="subtitle">Wygenerowano: {dt} &nbsp;|&nbsp; Czas: {meta['elapsed_min']} min &nbsp;|&nbsp; SMI({SMI_LEN_K},{SMI_LEN_D},{SMI_LEN_EMA})</p>
  <div class="strategy-box" style="margin:1.5rem 0">
    <strong>&#9881; Aktywna strategia:</strong>
    <ul>
      <li>Sygnal: <strong>Strong BUY</strong> lub <strong>Turning Up</strong></li>
      <li>Discount &ge; <strong>{int(MIN_DISCOUNT_52W*100)}%</strong> vs 52W High</li>
      <li>EPS TTM &gt; <strong>0</strong> (wymagane)</li>
      <li>Quick Ratio &ge; <strong>{MIN_QUICK}</strong> (wymagane)</li>
      <li>ROIC &gt; <strong>{int(MIN_ROIC*100)}%</strong> (wymagane)</li>
      <li>Debt/Equity &lt; <strong>{MAX_DEBT_EQUITY}</strong> (wymagane)</li>
      <li>Gross Margin &gt; <strong>{int(MIN_GROSS_MARGIN*100)}%</strong> (wymagane)</li>
      <li>Cap &gt; <strong>{MIN_MARKET_CAP//1_000_000}M</strong> | Vol &gt; <strong>{MIN_VOLUME:,}</strong></li>
      <li>Wyckoff W1: informacyjnie (badge W:Spring/SOS/&#9888;Dist)</li>
      <li>D1 Lead: informacyjnie (badge &#9889;D1 wczesniej / D1:-Xd)</li>
      <li>MTF Score: informacyjnie 0-5 (badge MTF&#9670;X, zgodnosc D1+W1+Monthly)</li>
      <li>Monthly Risk: ostrzezenie gdy trend miesieczny przeczy sygnalowi W1</li>
    </ul>
  </div>
  <div class="stats-bar">
    <div class="stat"><div class="stat-val">{meta['total_scanned']}</div><div class="stat-label">Przeskanowano</div></div>
    <div class="stat"><div class="stat-val">{meta['weekly_signals']}</div><div class="stat-label">Sygnalow SMI</div></div>
    <div class="stat"><div class="stat-val green">{meta['main_total']}</div><div class="stat-label">Po filtrach</div></div>
    <div class="stat"><div class="stat-val orange">{len(strong_res)}</div><div class="stat-label">Strong BUY</div></div>
    <div class="stat"><div class="stat-val purple">{len(turning_res)}</div><div class="stat-label">Turning Up</div></div>
    <div class="stat"><div class="stat-val" style="color:#ffb800">{len(early_res)}</div><div class="stat-label">Early Signal D1</div></div>
    <div class="stat"><div class="stat-val green">{meta.get('mtf_high_count', 0)}</div><div class="stat-label">MTF Score&ge;4</div></div>
    <div class="stat"><div class="stat-val" style="color:#ff4560">{meta.get('monthly_risk_count', 0)}</div><div class="stat-label">Monthly Risk</div></div>
  </div>
  <div class="section section-early">
    <div class="section-header"><span class="section-icon">&#9889;</span>
      <h2>Early Signal D1 &mdash; {len(early_res)} sygnalow</h2></div>
    <p class="early-note">Sp&oacute;&#322;ki, gdzie SMI dzienny ju&#380; pokaza&#322; crossover w g&oacute;r&#281;,
      ale SMI tygodniowy jeszcze go nie potwierdzi&#322; (strefa W1 nadal Bearish/OVERBOUGHT).
      To wczesne ostrze&#380;enie &mdash; sygna&#322; W1 mo&#380;e pojawi&#263; si&#281; za kilka tygodni,
      je&#347;li ruch na D1 si&#281; utrzyma. Brak filtr&oacute;w fundamentalnych w tej sekcji.</p>
    {render_cards(early_res, show_quality=False)}
  </div>
  <div class="section section-strong">
    <div class="section-header"><span class="section-icon">&#9889;</span>
      <h2>Strong BUY &mdash; {len(strong_res)} sygnalow</h2></div>
    {render_cards(strong_res, show_quality=True)}
  </div>
  <div class="section section-turning">
    <div class="section-header"><span class="section-icon">&#128260;</span>
      <h2>Turning Up &mdash; {len(turning_res)} sygnalow</h2></div>
    {render_cards(turning_res, show_quality=True)}
  </div>
  <div class="section">
    <div class="section-header"><span class="section-icon">&#128203;</span>
      <h2>Wszystkie wyniki &mdash; {len(results)}</h2></div>
    <div class="table-wrap"><table>
      <thead><tr>
        <th>Ticker</th><th>Nazwa</th><th>Rynek</th><th>Sektor</th>
        <th class="num">Cena</th><th class="num">Discount</th>
        <th class="num">Cap</th><th class="num">Vol</th>
        <th class="num">SMI W</th><th class="num">EMA W</th><th>Strefa</th>
        <th class="num">EPS</th><th class="num">QoQ</th><th class="num">YoY</th><th class="num">Sales</th><th class="num">QR</th>
        <th class="num">Wyckoff</th>
        <th class="num">ROIC</th><th class="num">D/E</th><th class="num">GM</th>
        <th class="num">Vol.W</th><th class="num">RS 12M</th><th class="num">Monthly</th>
      </tr></thead>
      <tbody>{render_table_rows(results, show_quality=True)}</tbody>
    </table></div>
  </div>
</div></body></html>"""
    path = f"{OUTPUT_DIR}/screener.html"
    with open(path,"w",encoding="utf-8") as f: f.write(html)
    print(f"  Raport glowny: {path}")

# ══════════════════════════════════════════════════════════════
#  HTML – FULL SCAN
# ══════════════════════════════════════════════════════════════

def generate_html_full(meta, results):
    dt = datetime.fromisoformat(meta["generated_at"]).strftime("%d.%m.%Y %H:%M")
    strong_res  = [r for r in results if r["signal"] == "Strong BUY"]
    buy_res     = [r for r in results if r["signal"] == "BUY"]
    turning_res = [r for r in results if r["signal"] == "Turning Up"]
    html = f"""<!DOCTYPE html>
<html lang="pl"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Full Scan SMI – {dt}</title>
<style>{COMMON_CSS}
  .info-box{{background:var(--bg2);border:1px solid #3ecf8e44;border-radius:10px;
             padding:.9rem 1.4rem;font-size:.82rem;line-height:1.7;margin-bottom:1.5rem}}
  .info-box strong{{color:var(--green)}}
</style></head><body>
<div class="page">
  <nav class="report-nav">
    <a href="index.html"     class="nav-link">&#127968; Start</a>
    <a href="screener.html"  class="nav-link">&#9889; Screener g&#322;&#243;wny</a>
    <a href="index_all.html" class="nav-link nav-link-active">&#128270; Full Scan</a>
  </nav>
  <h1>Full Scan &mdash; SMI Tygodniowy</h1>
  <p class="subtitle">Wygenerowano: {dt} &nbsp;|&nbsp; Czas: {meta['elapsed_min']} min</p>
  <div class="info-box">
    <strong>&#128270; Wszystkie sygnaly SMI</strong> bez filtrow fundamentalnych.
    Filtr: Cap &gt; {MIN_MARKET_CAP//1_000_000}M | Vol &gt; {MIN_VOLUME:,}.
    Kolumna <strong>Wyckoff</strong> informacyjna — Spring/SOS = faza akumulacji, &#9888;Dist = ostrzezenie.
  </div>
  <div class="stats-bar">
    <div class="stat"><div class="stat-val">{meta['total_scanned']}</div><div class="stat-label">Przeskanowano</div></div>
    <div class="stat"><div class="stat-val">{meta['weekly_signals']}</div><div class="stat-label">Sygnalow SMI</div></div>
    <div class="stat"><div class="stat-val green">{meta['full_total']}</div><div class="stat-label">Zebrano</div></div>
    <div class="stat"><div class="stat-val orange">{len(strong_res)}</div><div class="stat-label">Strong BUY</div></div>
    <div class="stat"><div class="stat-val blue">{len(buy_res)}</div><div class="stat-label">BUY</div></div>
    <div class="stat"><div class="stat-val purple">{len(turning_res)}</div><div class="stat-label">Turning Up</div></div>
  </div>
  <div class="section section-strong">
    <div class="section-header"><span class="section-icon">&#9889;</span>
      <h2>Strong BUY &mdash; {len(strong_res)}</h2></div>
    {render_cards(strong_res, show_quality=False)}
  </div>
  <div class="section section-buy">
    <div class="section-header"><span class="section-icon">&#9989;</span>
      <h2>BUY &mdash; {len(buy_res)}</h2></div>
    {render_cards(buy_res, show_quality=False)}
  </div>
  <div class="section section-turning">
    <div class="section-header"><span class="section-icon">&#128260;</span>
      <h2>Turning Up &mdash; {len(turning_res)}</h2></div>
    {render_cards(turning_res, show_quality=False)}
  </div>
  <div class="section">
    <div class="section-header"><span class="section-icon">&#128203;</span>
      <h2>Wszystkie wyniki &mdash; {len(results)}</h2></div>
    <div class="table-wrap"><table>
      <thead><tr>
        <th>Ticker</th><th>Nazwa</th><th>Rynek</th><th>Sektor</th>
        <th class="num">Cena</th><th class="num">Discount</th>
        <th class="num">Cap</th><th class="num">Vol</th>
        <th class="num">SMI W</th><th class="num">EMA W</th><th>Strefa</th>
        <th class="num">EPS*</th><th class="num">QoQ*</th><th class="num">YoY*</th><th class="num">Sales</th><th class="num">QR*</th>
        <th class="num">Wyckoff</th>
      </tr></thead>
      <tbody>{render_table_rows(results, show_quality=False)}</tbody>
    </table></div>
    <p style="font-size:.72rem;color:var(--muted);margin-top:.7rem">* dane informacyjne</p>
  </div>
</div></body></html>"""
    path = f"{OUTPUT_DIR}/index_all.html"
    with open(path,"w",encoding="utf-8") as f: f.write(html)
    print(f"  Raport full scan: {path}")

# ══════════════════════════════════════════════════════════════
#  HTML – STRONA STARTOWA
# ══════════════════════════════════════════════════════════════

def generate_html_index(meta):
    dt         = datetime.fromisoformat(meta["generated_at"]).strftime("%d.%m.%Y %H:%M")
    main_count = meta.get("main_total",0)
    full_count = meta.get("full_total",0)
    md         = meta.get("market_direction",{})
    usa_above  = md.get("usa_above_sma50w", True)
    eu_above   = md.get("eu_above_sma50w",  True)
    usa_price  = md.get("usa_price"); usa_sma = md.get("usa_sma50w")
    eu_price   = md.get("eu_price");  eu_sma  = md.get("eu_sma50w")

    def _trend_badge(above, price, sma, ticker):
        color = "#3ecf8e" if above else "#ff4560"
        arrow = "↑" if above else "↓"
        label = "TREND UP" if above else "TREND DOWN"
        detail = f"{price:.0f} vs SMA50W {sma:.0f}" if price and sma else ""
        return (f'<div style="display:flex;align-items:center;gap:.7rem;'
                f'background:rgba(255,255,255,.03);border:1px solid {color}33;'
                f'border-radius:8px;padding:.5rem 1rem;font-size:.78rem">'
                f'<span style="font-family:monospace;color:{color};font-weight:700;font-size:1rem">{arrow}</span>'
                f'<div><div style="color:#fff;font-weight:600">{ticker}</div>'
                f'<div style="color:{color};font-size:.7rem">{label}</div>'
                f'<div style="color:#555d7a;font-size:.67rem">{detail}</div></div></div>')

    html = f"""<!DOCTYPE html>
<html lang="pl"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Stock Screener SMI</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;700;800&display=swap');
  :root{{--bg:#080a14;--bg2:#0e1020;--border:#1c2040;--text:#c8cde8;--muted:#454a6a;
        --accent:#7c9ef0;--green:#3ecf8e;--orange:#ff6b00;--yellow:#ffb800;--purple:#c471ed}}
  *,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:'Syne',sans-serif;background:var(--bg);color:var(--text);
       min-height:100vh;display:flex;flex-direction:column;align-items:center;
       justify-content:center;padding:2rem;overflow-x:hidden}}
  body::before{{content:'';position:fixed;inset:0;
    background-image:linear-gradient(rgba(124,158,240,.04) 1px,transparent 1px),
                     linear-gradient(90deg,rgba(124,158,240,.04) 1px,transparent 1px);
    background-size:40px 40px;pointer-events:none;z-index:0}}
  .blob{{position:fixed;border-radius:50%;filter:blur(80px);pointer-events:none;z-index:0;animation:drift 12s ease-in-out infinite alternate}}
  .blob-1{{width:380px;height:380px;background:rgba(255,107,0,.08);top:-80px;right:-60px}}
  .blob-2{{width:300px;height:300px;background:rgba(62,207,142,.06);bottom:-60px;left:-40px;animation-delay:-5s}}
  .blob-3{{width:200px;height:200px;background:rgba(124,158,240,.07);top:50%;left:50%;transform:translate(-50%,-50%);animation-delay:-9s}}
  @keyframes drift{{from{{transform:translate(0,0) scale(1)}}to{{transform:translate(20px,15px) scale(1.08)}}}}
  .wrapper{{position:relative;z-index:1;text-align:center;max-width:700px;width:100%}}
  .badge{{display:inline-flex;align-items:center;gap:.45rem;background:rgba(124,158,240,.08);
          border:1px solid rgba(124,158,240,.2);border-radius:20px;padding:.3rem .9rem;
          font-family:'Space Mono',monospace;font-size:.72rem;color:var(--accent);
          letter-spacing:.04em;margin-bottom:1.6rem;animation:fadein .6s ease both}}
  .badge-dot{{width:6px;height:6px;background:var(--green);border-radius:50%;
              box-shadow:0 0 6px var(--green);animation:pulse 2s ease-in-out infinite}}
  @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
  h1{{font-size:clamp(2.2rem,6vw,3.4rem);font-weight:800;line-height:1.05;
      letter-spacing:-.03em;color:#fff;margin-bottom:.6rem;animation:fadein .6s .1s ease both}}
  h1 span{{background:linear-gradient(90deg,var(--orange),var(--yellow));
           -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text}}
  .sub{{font-size:.95rem;color:var(--muted);margin-bottom:3rem;line-height:1.6;animation:fadein .6s .2s ease both}}
  .sub code{{font-family:'Space Mono',monospace;font-size:.82rem;color:var(--accent);
             background:rgba(124,158,240,.08);padding:.1rem .4rem;border-radius:4px}}
  .cards{{display:grid;grid-template-columns:1fr 1fr;gap:1.2rem;margin-bottom:2.5rem;animation:fadein .6s .3s ease both}}
  .card{{position:relative;background:var(--bg2);border:1px solid var(--border);
         border-radius:16px;padding:1.8rem 1.5rem 1.6rem;text-decoration:none;
         color:var(--text);overflow:hidden;transition:transform .2s,border-color .2s,box-shadow .2s;text-align:left}}
  .card::before{{content:'';position:absolute;inset:0;opacity:0;transition:opacity .25s;border-radius:16px}}
  .card:hover{{transform:translateY(-4px)}}
  .card-main::before{{background:radial-gradient(circle at 30% 20%,rgba(255,107,0,.18),transparent 65%)}}
  .card-full::before{{background:radial-gradient(circle at 30% 20%,rgba(62,207,142,.12),transparent 65%)}}
  .card:hover::before{{opacity:1}}
  .card-main{{border-color:rgba(255,107,0,.25)}} .card-full{{border-color:rgba(62,207,142,.2)}}
  .card-main:hover{{border-color:var(--orange);box-shadow:0 8px 32px rgba(255,107,0,.12)}}
  .card-full:hover{{border-color:var(--green);box-shadow:0 8px 32px rgba(62,207,142,.1)}}
  .card-bar{{position:absolute;top:0;left:0;right:0;height:3px;border-radius:16px 16px 0 0}}
  .card-main .card-bar{{background:linear-gradient(90deg,var(--orange),var(--yellow))}}
  .card-full .card-bar{{background:linear-gradient(90deg,var(--green),var(--accent))}}
  .card-icon{{font-size:1.8rem;margin-bottom:.9rem;display:block}}
  .card-title{{font-size:1.15rem;font-weight:700;color:#fff;margin-bottom:.35rem}}
  .card-desc{{font-size:.8rem;color:var(--muted);line-height:1.55;margin-bottom:.8rem}}
  .card-count{{font-family:'Space Mono',monospace;font-size:.78rem;font-weight:700;margin-bottom:.9rem}}
  .card-main .card-count{{color:var(--orange)}} .card-full .card-count{{color:var(--green)}}
  .card-tags{{display:flex;flex-wrap:wrap;gap:.4rem}}
  .tag{{font-family:'Space Mono',monospace;font-size:.65rem;padding:.2rem .55rem;border-radius:4px;font-weight:700;letter-spacing:.03em}}
  .tag-orange{{background:rgba(255,107,0,.12);color:var(--orange)}}
  .tag-green {{background:rgba(62,207,142,.1);color:var(--green)}}
  .tag-blue  {{background:rgba(124,158,240,.1);color:var(--accent)}}
  .tag-purple{{background:rgba(196,113,237,.1);color:var(--purple)}}
  .card-arrow{{position:absolute;bottom:1.4rem;right:1.4rem;font-size:1rem;color:var(--muted);transition:color .2s,transform .2s}}
  .card:hover .card-arrow{{color:#fff;transform:translate(3px,-3px)}}
  .info-bar{{display:flex;justify-content:center;gap:2rem;flex-wrap:wrap;animation:fadein .6s .45s ease both}}
  .info-item{{display:flex;align-items:center;gap:.5rem;font-family:'Space Mono',monospace;font-size:.72rem;color:var(--muted)}}
  .info-item span:first-child{{color:var(--accent)}}
  @keyframes fadein{{from{{opacity:0;transform:translateY(14px)}}to{{opacity:1;transform:translateY(0)}}}}
  @media(max-width:560px){{.cards{{grid-template-columns:1fr}}h1{{font-size:2rem}}}}
</style></head><body>
<div class="blob blob-1"></div><div class="blob blob-2"></div><div class="blob blob-3"></div>
<div class="wrapper">
  <div class="badge"><div class="badge-dot"></div>SMI(10,3,3) &nbsp;&middot;&nbsp; Interwal tygodniowy</div>
  <h1>Stock<br><span>Screener</span></h1>
  <p class="sub">Skanuje rynki USA i EU w poszukiwaniu sygnalow<br>
     wskaznika <code>Stochastic Momentum Index</code><br>
     <span style="font-size:.8rem">Ostatni skan: {dt}</span></p>
  <div style="display:flex;gap:.8rem;justify-content:center;margin-bottom:2rem;flex-wrap:wrap;animation:fadein .6s .25s ease both">
    {_trend_badge(usa_above,usa_price,usa_sma,"SPY (USA)")}
    {_trend_badge(eu_above, eu_price, eu_sma, "VGK (EU)")}
  </div>
  <div class="cards">
    <a href="screener.html" class="card card-main">
      <div class="card-bar"></div><span class="card-icon">&#9889;</span>
      <div class="card-title">Screener glowny</div>
      <div class="card-desc">Strong BUY i Turning Up z pelnym zestawem filtrow fundamentalnych.</div>
      <div class="card-count">&#9662; {main_count} wynikow</div>
      <div class="card-tags">
        <span class="tag tag-orange">Strong BUY</span><span class="tag tag-purple">Turning Up</span>
        <span class="tag tag-blue">ROIC&gt;15%</span><span class="tag tag-blue">D/E&lt;1</span>
        <span class="tag tag-green">Wyckoff W1</span>
      </div><div class="card-arrow">&#8599;</div>
    </a>
    <a href="index_all.html" class="card card-full">
      <div class="card-bar"></div><span class="card-icon">&#128270;</span>
      <div class="card-title">Full Scan</div>
      <div class="card-desc">Wszystkie sygnaly SMI bez filtrow. Wyckoff jako kolumna informacyjna.</div>
      <div class="card-count">&#9662; {full_count} wynikow</div>
      <div class="card-tags">
        <span class="tag tag-orange">Strong BUY</span><span class="tag tag-green">BUY</span>
        <span class="tag tag-purple">Turning Up</span><span class="tag tag-blue">Cap&gt;200M</span>
        <span class="tag tag-green">Wyckoff W1</span>
      </div><div class="card-arrow">&#8599;</div>
    </a>
  </div>
  <div class="info-bar">
    <div class="info-item"><span>&#9670;</span> S&amp;P 500 + NASDAQ + NYSE + AMEX</div>
    <div class="info-item"><span>&#9670;</span> DAX &middot; CAC &middot; FTSE &middot; AEX &middot; WIG + inne</div>
    <div class="info-item"><span>&#9670;</span> Aktualizacja: GitHub Actions</div>
  </div>
</div></body></html>"""
    path = f"{OUTPUT_DIR}/index.html"
    with open(path,"w",encoding="utf-8") as f: f.write(html)
    print(f"  Strona startowa: {path}")

# ══════════════════════════════════════════════════════════════
#  LISTY TRADINGVIEW
# ══════════════════════════════════════════════════════════════

_TV_SUFFIX_MAP = {
    ".DE":"XETR",".PA":"EURONEXT",".L":"LSE",".AS":"EURONEXT",".MC":"BME",
    ".SW":"SIX",".MI":"MIL",".ST":"OM",".OL":"OSL",".BR":"EURONEXT",".WA":"GPW",
}

def _to_tv_ticker(yahoo_ticker):
    for suffix, exchange in _TV_SUFFIX_MAP.items():
        if yahoo_ticker.endswith(suffix):
            return f"{exchange}:{yahoo_ticker[:-len(suffix)]}"
    return yahoo_ticker

def generate_tradingview_lists(main_results, full_results):
    dt_str = datetime.now().strftime("%Y-%m-%d %H:%M UTC")
    for label, data, filename in [
        ("Screener główny", main_results, "tv_main.txt"),
        ("Full Scan",       full_results, "tv_all.txt"),
    ]:
        sorted_data = sorted(data, key=lambda x: (
            {"Strong BUY":0,"BUY":1,"Turning Up":2}.get(x["signal"],9),
            -(x.get("discount_52w") or 0),
        ))
        lines = [
            f"### TradingView Watchlist – {label}",
            f"### Wygenerowano: {dt_str}",
            f"### Liczba tickerow: {len(sorted_data)}",
            "###",
        ]
        for sig_type in ("Strong BUY","BUY","Turning Up"):
            group = [r for r in sorted_data if r["signal"] == sig_type]
            if not group: continue
            lines.append(f"### ── {sig_type} ({len(group)}) ──")
            for r in group:
                tv    = _to_tv_ticker(r["ticker"])
                disc  = f"-{r['discount_52w']}%" if r.get("discount_52w") is not None else "--"
                wyk_s = r.get("wyckoff_score", 0)
                wyk_d = r.get("wyckoff_dist", False)
                wyk_l, _ = _wyckoff_cell(wyk_s, wyk_d)
                mtf   = r.get("mtf_score")
                mtf_s = f"MTF:{mtf}/5" if mtf is not None else "MTF:--"
                mrisk = " M:RISK" if r.get("m_risk_warning") else ""
                lines.append(
                    f"{tv}  ### {r['signal']} | {disc} | W:{wyk_l} | {mtf_s}{mrisk} | {r.get('sector','--')}"
                )
        path = f"{OUTPUT_DIR}/{filename}"
        with open(path,"w",encoding="utf-8") as f: f.write("\n".join(lines)+"\n")
        clean_path = path.replace(".txt","_clean.txt")
        with open(clean_path,"w",encoding="utf-8") as f:
            f.write("\n".join(_to_tv_ticker(r["ticker"]) for r in sorted_data)+"\n")
        print(f"  TradingView {label}: {path}  ({len(sorted_data)} tickerow)")

# ══════════════════════════════════════════════════════════════
#  GŁÓWNA PĘTLA
# ══════════════════════════════════════════════════════════════

def run_screener():
    t0 = datetime.now()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print("="*60)
    print(f"SCREENER START: {t0.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"SMI({SMI_LEN_K},{SMI_LEN_D},{SMI_LEN_EMA}) | Wyckoff W1 (akumulacja + dystrybucja)")
    print(f"Cache fundamentow: {CACHE_DIR}  (TTL {CACHE_TTL_H}h)")
    print("="*60)

    print("\n[Tickery] Pobieranie list spolek (rownolegly)...")
    with ThreadPoolExecutor(max_workers=4) as pool:
        f_sp500  = pool.submit(get_sp500)
        f_nasdaq = pool.submit(get_nasdaq)
        f_nyse   = pool.submit(get_nyse_amex)
        f_eu     = pool.submit(get_european_indices)
        usa = list(set(f_sp500.result() + f_nasdaq.result() + f_nyse.result()))
        eu  = list(set(f_eu.result()))
    ticker_market = [(t,"USA") for t in usa] + [(t,"EU") for t in eu]
    print(f"\nLacznie: {len(ticker_market)} ({len(usa)} USA, {len(eu)} EU)")

    print("\n[Market Direction + Dane W1] Start rownolegly...")
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_mdir   = pool.submit(check_market_direction)
        f_weekly = pool.submit(phase1_weekly_signals, ticker_market)
        market_dir     = f_mdir.result()
        weekly_signals = f_weekly.result()
    if not market_dir["USA"] and not market_dir["EU"]:
        print("  Oba rynki ponizej SMA50W — sygnaly SMI maja nizsza wiarygodnosc!")
    all_data       = phase2_collect(weekly_signals)

    main_results = [r for r in all_data if filter_main(r)]
    full_results = all_data

    # ── FAZA 3: Monthly SMI + MTF Score (tylko dla finalnej listy main_results) ──
    mtf_high_count = 0
    monthly_risk_count = 0
    if main_results:
        print(f"\n[3/3] SMI miesieczny (Monthly) + MTF Score -- "
              f"{len(main_results)} tickerow (finalna lista)...")
        m_tickers = [r["ticker"] for r in main_results]
        monthly_data = bulk_download(m_tickers, period="10y", interval="1mo")
        print(f"      Pobrano Monthly: {len(monthly_data)}")

        for r in main_results:
            df_m = monthly_data.get(r["ticker"])
            m_info = calc_monthly_zone(df_m)
            r["m_zone"]         = m_info["m_zone"]
            r["m_bullish"]      = m_info["m_bullish"]
            r["m_risk_warning"] = m_info["m_risk_warning"]
            r["mtf_score"]      = calc_mtf_score(r)
            if r["mtf_score"] >= 4:
                mtf_high_count += 1
            if r["m_risk_warning"]:
                monthly_risk_count += 1

        main_results.sort(
            key=lambda x: (
                {"Strong BUY": 0, "Turning Up": 1}.get(x.get("signal", ""), 2),
                -(x.get("tech_score", 0) * 2 + (x.get("mtf_score") or 0)),
            )
        )
        print(f"      MTF Score>=4: {mtf_high_count} spolek | "
              f"Monthly Risk Warning: {monthly_risk_count} spolek | "
              f"Posortowano po tech_score*2 + mtf_score")
    # full_results nie dostaje Monthly/MTF (informacyjne tylko na liście głównej)
    for r in full_results:
        r.setdefault("m_zone", "--")
        r.setdefault("m_bullish", False)
        r.setdefault("m_risk_warning", False)
        r.setdefault("mtf_score", None)

    elapsed = round((datetime.now()-t0).total_seconds()/60, 1)
    medef run_screener():
    t0 = datetime.now()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print("="*60)
    print(f"SCREENER START: {t0.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"SMI({SMI_LEN_K},{SMI_LEN_D},{SMI_LEN_EMA}) | Wyckoff W1 (akumulacja + dystrybucja)")
    print(f"Cache fundamentow: {CACHE_DIR}  (TTL {CACHE_TTL_H}h)")
    print("="*60)

    print("\n[Tickery] Pobieranie list spolek (rownolegly)...")
    with ThreadPoolExecutor(max_workers=4) as pool:
        f_sp500  = pool.submit(get_sp500)
        f_nasdaq = pool.submit(get_nasdaq)
        f_nyse   = pool.submit(get_nyse_amex)
        f_eu     = pool.submit(get_european_indices)
        usa = list(set(f_sp500.result() + f_nasdaq.result() + f_nyse.result()))
        eu  = list(set(f_eu.result()))
    ticker_market = [(t,"USA") for t in usa] + [(t,"EU") for t in eu]
    print(f"\nLacznie: {len(ticker_market)} ({len(usa)} USA, {len(eu)} EU)")

    print("\n[Market Direction + Dane W1] Start rownolegly...")
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_mdir   = pool.submit(check_market_direction)
        f_weekly = pool.submit(phase1_weekly_signals, ticker_market)
        market_dir     = f_mdir.result()
        weekly_signals = f_weekly.result()
    if not market_dir["USA"] and not market_dir["EU"]:
        print("  Oba rynki ponizej SMA50W — sygnaly SMI maja nizsza wiarygodnosc!")
    all_data       = phase2_collect(weekly_signals)

    main_results = [r for r in all_data if filter_main(r)]
    full_results = all_data

    # ── FAZA 3: Monthly SMI + MTF Score (tylko dla finalnej listy main_results) ──
    mtf_high_count = 0
    monthly_risk_count = 0
    if main_results:
        print(f"\n[3/3] SMI miesieczny (Monthly) + MTF Score -- "
              f"{len(main_results)} tickerow (finalna lista)...")
        m_tickers = [r["ticker"] for r in main_results]
        monthly_data = bulk_download(m_tickers, period="10y", interval="1mo")
        print(f"      Pobrano Monthly: {len(monthly_data)}")

        for r in main_results:
            df_m = monthly_data.get(r["ticker"])
            m_info = calc_monthly_zone(df_m)
            r["m_zone"]         = m_info["m_zone"]
            r["m_bullish"]      = m_info["m_bullish"]
            r["m_risk_warning"] = m_info["m_risk_warning"]
            r["mtf_score"]      = calc_mtf_score(r)
            if r["mtf_score"] >= 4:
                mtf_high_count += 1
            if r["m_risk_warning"]:
                monthly_risk_count += 1

        main_results.sort(
            key=lambda x: (
                {"Strong BUY": 0, "Turning Up": 1}.get(x.get("signal", ""), 2),
                -(x.get("tech_score", 0) * 2 + (x.get("mtf_score") or 0)),
            )
        )
        print(f"      MTF Score>=4: {mtf_high_count} spolek | "
              f"Monthly Risk Warning: {monthly_risk_count} spolek | "
              f"Posortowano po tech_score*2 + mtf_score")
    # full_results nie dostaje Monthly/MTF (informacyjne tylko na liście głównej)
    for r in full_results:
        r.setdefault("m_zone", "--")
        r.setdefault("m_bullish", False)
        r.setdefault("m_risk_warning", False)
        r.setdefault("mtf_score", None)

    elapsed = round((datetime.now()-t0).total_seconds()/60, 1)
    meta = {
        "generated_at":   datetime.now().isoformat(),
        "elapsed_min":    elapsed,
        "total_scanned":  len(ticker_market),
        "weekly_signals": len(weekly_signals),
        "main_total":     len(main_results),
        "full_total":     len(full_results),
        "mtf_high_count":     mtf_high_count,
        "monthly_risk_count": monthly_risk_count,
        "indicator":      f"SMI({SMI_LEN_K},{SMI_LEN_D},{SMI_LEN_EMA})",
        "market_direction": {
            "usa_above_sma50w": market_dir["USA"],
            "eu_above_sma50w":  market_dir["EU"],
            "usa_price":        market_dir.get("usa_price"),
            "usa_sma50w":       market_dir.get("usa_sma50w"),
            "eu_price":         market_dir.get("eu_price"),
            "eu_sma50w":        market_dir.get("eu_sma50w"),
        },
    }

    def _save_outputs():
        for fname, d in [("results",full_results),("results_main",main_results),("meta",meta)]:
            with open(f"{OUTPUT_DIR}/{fname}.json","w",encoding="utf-8") as f:
                json.dump(d, f, indent=2, ensure_ascii=False)
        if full_results:  pd.DataFrame(full_results).to_csv(f"{OUTPUT_DIR}/results.csv",index=False)
        if main_results:  pd.DataFrame(main_results).to_csv(f"{OUTPUT_DIR}/results_main.csv",index=False)

    def _generate_html():
        print("\n[HTML] Generowanie raportow...")
        generate_html_main(meta, main_results)
        generate_html_full(meta, full_results)
        generate_html_index(meta)

    def _generate_tv():
        print("\n[TV] Listy TradingView...")
        generate_tradingview_lists(main_results, full_results)

    # OPT 5: zapis JSON/CSV, HTML i TV równolegle
    # WAŻNE: .result() na każdym future jest konieczne — bez tego wyjątki
    # rzucone w wątkach są całkowicie połykane i proces "kończy się sukcesem"
    # mimo że nic nie zostało zapisane. To był realny bug (patrz: incydent
    # z 30-sekundowym runem i pustymi wynikami).
    with ThreadPoolExecutor(max_workers=3) as pool:
        f_save = pool.submit(_save_outputs)
        f_html = pool.submit(_generate_html)
        f_tv   = pool.submit(_generate_tv)
        f_save.result()
        f_html.result()
        f_tv.result()

    print(f"\nCzas lacznie: {elapsed} min")
    print(f"Screener glowny : {len(main_results)} wynikow")
    print(f"Full Scan       : {len(full_results)} wynikow")
    return main_results, full_results

if __name__ == "__main__":
    run_screener()
