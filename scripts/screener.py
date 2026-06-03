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
"""

import yfinance as yf
import pandas as pd
import numpy as np
import requests
import json
import os
from datetime import datetime
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed

# ══════════════════════════════════════════════════════════════
#  KONFIGURACJA
# ══════════════════════════════════════════════════════════════
MIN_MARKET_CAP   = 200_000_000
MIN_VOLUME       = 300_000

# Filtry screener główny
MIN_QUICK        = 1.0
MIN_DISCOUNT_52W = 0.30
MIN_ROIC         = 0.15    # 15%
MAX_DEBT_EQUITY  = 1.0     # < 1
MIN_GROSS_MARGIN = 0.30    # 30%

FUNDAMENTALS_WORKERS = 20
DOWNLOAD_BATCH_SIZE  = 100
OUTPUT_DIR           = "results"
SMI_LEN_K, SMI_LEN_D, SMI_LEN_EMA = 10, 3, 3

# ══════════════════════════════════════════════════════════════
#  POBIERANIE TICKERÓW
# ══════════════════════════════════════════════════════════════

def get_sp500():
    try:
        url = ("https://raw.githubusercontent.com/datasets/"
               "s-and-p-500-companies/main/data/constituents.csv")
        r  = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        df = pd.read_csv(StringIO(r.text))
        tickers = (df["Symbol"].dropna().str.strip()
                   .str.replace(".", "-", regex=False).tolist())
        print(f"  S&P 500: {len(tickers)}")
        return tickers
    except Exception as e:
        print(f"  S&P 500 blad: {e}"); return []

def get_nasdaq():
    import re
    try:
        url = ("https://raw.githubusercontent.com/rreichel3/"
               "US-Stock-Symbols/main/nasdaq/nasdaq_tickers.json")
        r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        tickers = [t.strip() for t in r.json() if re.match(r'^[A-Z]{1,5}$', t.strip())]
        print(f"  NASDAQ: {len(tickers)}")
        return tickers
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
    print(f"  EU: {len(all_eu)} tickerow "
          f"(DAX:{len(dax)} CAC:{len(cac)} FTSE:{len(ftse)} AEX:{len(aex)} "
          f"IBEX:{len(ibex)} SMI:{len(smi_idx)} MIB:{len(mib)} "
          f"OMX:{len(omx)} OBX:{len(obx)} BEL:{len(bel)} WIG:{len(wig)})")
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

def detect_bullish_divergence(close: pd.Series, smi: pd.Series,
                               lookback: int = 14) -> tuple:
    """
    Wykrywa dywergencję byczą między ceną a SMI.
    Zwraca (bool, opis).

    Warunek klasyczny:
      - Cena: low2 < low1  (nowy dołek cenowy)
      - SMI:  smi2 > smi1  (wyższy dołek SMI = brak potwierdzenia)
      gdzie indeks 1 = starszy dołek, 2 = nowszy dołek
    """
    if len(close) < lookback + 5 or len(smi) < lookback + 5:
        return False, ""
    c = close.values[-lookback:]
    s = smi.values[-lookback:]
    # Szukaj lokalnych minimów ceny (okno ±2 świece)
    price_lows = []
    for i in range(2, len(c) - 2):
        if c[i] < c[i-1] and c[i] < c[i+1] and c[i] < c[i-2] and c[i] < c[i+2]:
            price_lows.append((i, float(c[i]), float(s[i])))
    if len(price_lows) < 2:
        return False, ""
    p1_idx, p1_price, p1_smi = price_lows[-2]
    p2_idx, p2_price, p2_smi = price_lows[-1]
    # Dywergencja bycza: cena niżej, SMI wyżej
    if p2_price < p1_price and p2_smi > p1_smi:
        delta_price = round((p2_price - p1_price) / p1_price * 100, 1)
        delta_smi   = round(p2_smi - p1_smi, 1)
        desc = f"div_bull (cena {delta_price}%, SMI +{delta_smi})"
        return True, desc
    return False, ""


def smi_weekly_signal(smi, smi_ema):
    if len(smi) < 4:
        return None, None, None, "--"
    s0 = float(smi.iloc[-1]);  s1 = float(smi.iloc[-2])
    s2 = float(smi.iloc[-3]);  e0 = float(smi_ema.iloc[-1])
    e1 = float(smi_ema.iloc[-2])
    if any(np.isnan(v) for v in [s0, s1, s2, e0, e1]):
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

# ══════════════════════════════════════════════════════════════
#  BULK DOWNLOAD
# ══════════════════════════════════════════════════════════════

def bulk_download(tickers, period, interval):
    result  = {}
    batches = [tickers[i:i+DOWNLOAD_BATCH_SIZE]
               for i in range(0, len(tickers), DOWNLOAD_BATCH_SIZE)]
    for batch in batches:
        if not batch: continue
        try:
            if len(batch) == 1:
                raw = yf.download(batch[0], period=period, interval=interval,
                                  auto_adjust=True, progress=False)
                if raw is not None and not raw.empty:
                    result[batch[0]] = raw
            else:
                raw = yf.download(batch, period=period, interval=interval,
                                  group_by="ticker", auto_adjust=True,
                                  progress=False, threads=True)
                if raw is None or raw.empty: continue
                for ticker in batch:
                    try:
                        df = raw[ticker].dropna(how="all")
                        if not df.empty and len(df) >= SMI_LEN_K + 5:
                            result[ticker] = df
                    except Exception:
                        pass
        except Exception:
            pass
    return result

# ══════════════════════════════════════════════════════════════
#  FAZA 1 – Tygodniowe sygnały SMI
# ══════════════════════════════════════════════════════════════

def phase1_weekly_signals(ticker_market_list):
    tickers    = [t for t, _ in ticker_market_list]
    market_map = {t: m for t, m in ticker_market_list}
    print(f"\n[1/2] Dane tygodniowe -- {len(tickers)} tickerow...")
    data = bulk_download(tickers, period="2y", interval="1wk")
    print(f"      Pobrano: {len(data)}")
    signals = {}
    for ticker, df in data.items():
        try:
            smi, smi_e = calc_smi(df["High"], df["Low"], df["Close"],
                                  SMI_LEN_K, SMI_LEN_D, SMI_LEN_EMA)
            sig, s_val, e_val, zone = smi_weekly_signal(smi, smi_e)
            if sig is not None:
                div_bull, div_desc = detect_bullish_divergence(df["Close"], smi)
                signals[ticker] = {
                    "market": market_map[ticker],
                    "smi": s_val, "smi_ema": e_val,
                    "zone": zone, "signal": sig,
                    "divergence_bull": div_bull,
                    "divergence_desc": div_desc,
                }
        except Exception:
            pass
    s  = sum(1 for v in signals.values() if v["signal"] == "Strong BUY")
    b  = sum(1 for v in signals.values() if v["signal"] == "BUY")
    tu = sum(1 for v in signals.values() if v["signal"] == "Turning Up")
    print(f"      Sygnaly: {len(signals)} (Strong BUY:{s}  BUY:{b}  Turning Up:{tu})")
    return signals

# ══════════════════════════════════════════════════════════════
#  FAZA 2 – Dane meta + fundamenty
# ══════════════════════════════════════════════════════════════

def _calc_roic(tkr):
    """
    ROIC = NOPAT / Invested Capital
    NOPAT  = Operating Income * (1 - tax_rate)
    IC     = Total Assets - Current Liabilities - Cash
    tax_rate domyślnie 21% jeśli brak danych
    """
    try:
        fin = tkr.financials
        bs  = tkr.balance_sheet
        if fin is None or bs is None or fin.empty or bs.empty:
            return None

        # Operating Income (EBIT)
        ebit = None
        for label in ["Operating Income", "EBIT", "Earnings Before Interest And Taxes"]:
            if label in fin.index:
                v = fin.loc[label].dropna()
                if len(v) >= 1:
                    ebit = float(v.iloc[0])
                break
        if ebit is None:
            return None

        # Tax rate z rachunku zysków i strat
        tax_rate = 0.21
        try:
            tax_prov, pretax = None, None
            for lbl in ["Tax Provision", "Income Tax Expense"]:
                if lbl in fin.index:
                    v = fin.loc[lbl].dropna()
                    if len(v) >= 1: tax_prov = float(v.iloc[0])
                    break
            for lbl in ["Pretax Income", "Income Before Tax"]:
                if lbl in fin.index:
                    v = fin.loc[lbl].dropna()
                    if len(v) >= 1: pretax = float(v.iloc[0])
                    break
            if tax_prov is not None and pretax and pretax != 0:
                tax_rate = max(0.0, min(0.5, tax_prov / pretax))
        except Exception:
            pass

        nopat = ebit * (1 - tax_rate)

        # Invested Capital = Total Assets - Current Liabilities - Cash
        ta, cl, cash = None, 0.0, 0.0
        for lbl in ["Total Assets"]:
            if lbl in bs.index:
                v = bs.loc[lbl].dropna()
                if len(v) >= 1: ta = float(v.iloc[0])
                break
        for lbl in ["Current Liabilities", "Total Current Liabilities"]:
            if lbl in bs.index:
                v = bs.loc[lbl].dropna()
                if len(v) >= 1: cl = float(v.iloc[0])
                break
        for lbl in ["Cash And Cash Equivalents", "Cash", "Cash Cash Equivalents And Short Term Investments"]:
            if lbl in bs.index:
                v = bs.loc[lbl].dropna()
                if len(v) >= 1: cash = float(v.iloc[0])
                break

        if ta is None or ta == 0:
            return None
        ic = ta - cl - cash
        if ic <= 0:
            return None

        return round(nopat / ic, 4)
    except Exception:
        return None


def _calc_debt_equity(tkr):
    """
    Debt/Equity = Total Debt / Stockholders Equity
    """
    try:
        bs = tkr.balance_sheet
        if bs is None or bs.empty:
            return None

        debt, equity = None, None
        for lbl in ["Total Debt", "Long Term Debt"]:
            if lbl in bs.index:
                v = bs.loc[lbl].dropna()
                if len(v) >= 1: debt = float(v.iloc[0])
                break
        # Jeśli brak Total Debt – suma long i short term
        if debt is None:
            ltd, std = 0.0, 0.0
            for lbl in ["Long Term Debt", "Long Term Debt And Capital Lease Obligation"]:
                if lbl in bs.index:
                    v = bs.loc[lbl].dropna()
                    if len(v) >= 1: ltd = float(v.iloc[0])
                    break
            for lbl in ["Current Debt", "Short Term Debt", "Short Long Term Debt"]:
                if lbl in bs.index:
                    v = bs.loc[lbl].dropna()
                    if len(v) >= 1: std = float(v.iloc[0])
                    break
            debt = ltd + std

        for lbl in ["Stockholders Equity", "Total Stockholders Equity",
                    "Common Stock Equity", "Total Equity Gross Minority Interest"]:
            if lbl in bs.index:
                v = bs.loc[lbl].dropna()
                if len(v) >= 1: equity = float(v.iloc[0])
                break

        if equity is None or equity <= 0 or debt is None:
            return None
        return round(debt / equity, 3)
    except Exception:
        return None


def _calc_gross_margin(tkr):
    """
    Gross Margin = Gross Profit / Revenue
    """
    try:
        fin = tkr.financials
        if fin is None or fin.empty:
            return None

        gp, rev = None, None
        for lbl in ["Gross Profit"]:
            if lbl in fin.index:
                v = fin.loc[lbl].dropna()
                if len(v) >= 1: gp = float(v.iloc[0])
                break
        for lbl in ["Total Revenue", "Operating Revenue"]:
            if lbl in fin.index:
                v = fin.loc[lbl].dropna()
                if len(v) >= 1: rev = float(v.iloc[0])
                break

        if gp is None or rev is None or rev == 0:
            return None
        return round(gp / rev, 4)
    except Exception:
        return None


def _collect_one(symbol, weekly_data):
    """Pobiera dane dla tickera. Zwraca None jeśli nie spełnia
    minimalnych warunków płynności (cap>200M, vol>300K)."""
    try:
        tkr = yf.Ticker(symbol)
        fi  = tkr.fast_info

        price    = getattr(fi, "last_price", None)
        cap      = getattr(fi, "market_cap", None)
        vol      = (getattr(fi, "three_month_average_volume", None)
                    or getattr(fi, "last_volume", None))
        currency = getattr(fi, "currency", "USD")
        high_52w = getattr(fi, "year_high", None)

        # Filtr płynności – wspólny dla obu raportów
        if not cap or cap < MIN_MARKET_CAP: return None
        if not vol or vol < MIN_VOLUME:     return None

        discount_pct = None
        if high_52w and price and high_52w > 0:
            discount_pct = round((high_52w - price) / high_52w * 100, 1)

        name = symbol; sector = "--"; country = "--"
        eps_ttm = None; sales = None; qr = None

        try:
            info = tkr.info
            if info and len(info) > 5:
                name    = info.get("shortName") or symbol
                sector  = info.get("sector")    or "--"
                country = info.get("country")   or "--"
                eps     = info.get("trailingEps")
                if eps is not None: eps_ttm = round(float(eps), 2)
        except Exception:
            pass

        # Pobierz sprawozdania raz – używane przez QR, Sales, ROIC, D/E, GM
        try:
            fin = tkr.financials
            bs  = tkr.balance_sheet
        except Exception:
            fin, bs = None, None

        # Sales TTM
        try:
            if fin is not None and not fin.empty:
                for label in ["Total Revenue", "Operating Revenue"]:
                    if label in fin.index:
                        rev = fin.loc[label].dropna()
                        if len(rev) >= 1:
                            sales = round(float(rev.iloc[0]) / 1e6, 1)
                        break
        except Exception:
            pass

        # Quick Ratio
        try:
            if bs is not None and not bs.empty:
                ca, inv, cl = None, 0.0, None
                for label in ["Current Assets", "Total Current Assets"]:
                    if label in bs.index:
                        v = bs.loc[label].dropna()
                        if len(v) >= 1: ca = float(v.iloc[0])
                        break
                for label in ["Inventory", "Inventories"]:
                    if label in bs.index:
                        v = bs.loc[label].dropna()
                        if len(v) >= 1: inv = float(v.iloc[0])
                        break
                for label in ["Current Liabilities", "Total Current Liabilities"]:
                    if label in bs.index:
                        v = bs.loc[label].dropna()
                        if len(v) >= 1: cl = float(v.iloc[0])
                        break
                if ca is not None and cl is not None and cl > 0:
                    qr = round((ca - inv) / cl, 2)
        except Exception:
            pass

        # ── NOWE METRYKI ──────────────────────────────────────
        roic         = _calc_roic(tkr)
        debt_equity  = _calc_debt_equity(tkr)
        gross_margin = _calc_gross_margin(tkr)
        # ──────────────────────────────────────────────────────

        return {
            "ticker":         symbol,
            "name":           name,
            "market":         weekly_data["market"],
            "country":        country,
            "sector":         sector,
            "price":          round(price, 2) if price else None,
            "currency":       currency,
            "high_52w":       round(high_52w, 2) if high_52w else None,
            "discount_52w":   discount_pct,
            "market_cap_mln": round(cap / 1e6, 1),
            "volume_k":       round(vol / 1000, 1),
            "smi":            weekly_data["smi"],
            "smi_ema":        weekly_data["smi_ema"],
            "zone":           weekly_data["zone"],
            "signal":         weekly_data["signal"],
            "divergence_bull": weekly_data.get("divergence_bull", False),
            "divergence_desc": weekly_data.get("divergence_desc", ""),
            "eps_ttm":        eps_ttm,
            "sales_ttm_mln":  sales,
            "quick_ratio":    qr,
            "roic":           roic,
            "debt_equity":    debt_equity,
            "gross_margin":   gross_margin,
            "scanned_at":     datetime.now().isoformat(),
        }
    except Exception:
        return None


def phase2_collect(weekly_signals):
    if not weekly_signals: return []
    candidates = list(weekly_signals.keys())
    print(f"\n[2/2] Meta + fundamenty -- {len(candidates)} tickerow "
          f"({FUNDAMENTALS_WORKERS} watkow)...")
    results = []
    with ThreadPoolExecutor(max_workers=FUNDAMENTALS_WORKERS) as pool:
        futures = {
            pool.submit(_collect_one, sym, weekly_signals[sym]): sym
            for sym in candidates
        }
        for future in as_completed(futures):
            r = future.result()
            if r: results.append(r)
    print(f"      Zebrano danych: {len(results)}")
    # Oblicz scoring techniczny i posortuj malejąco
    for r in results:
        r["tech_score"] = calc_tech_score(r)
    results.sort(key=lambda x: x.get("tech_score", 0), reverse=True)
    return results

# ══════════════════════════════════════════════════════════════
#  FILTRY
# ══════════════════════════════════════════════════════════════

def filter_main(r):
    """Filtry screener główny: Strong BUY / Turning Up + fundamenty + discount."""
    if r["signal"] not in ("Strong BUY", "Turning Up"):
        return False
    if r.get("eps_ttm") is not None and r["eps_ttm"] < 0:
        return False
    if r.get("quick_ratio") is not None and r["quick_ratio"] < MIN_QUICK:
        return False
    disc = r.get("discount_52w")
    if disc is not None and disc < MIN_DISCOUNT_52W * 100:
        return False
    # ── NOWE FILTRY ───────────────────────────────────────────
    roic = r.get("roic")
    if roic is not None and roic < MIN_ROIC:
        return False
    de = r.get("debt_equity")
    if de is not None and de > MAX_DEBT_EQUITY:
        return False
    gm = r.get("gross_margin")
    if gm is not None and gm < MIN_GROSS_MARGIN:
        return False
    # ─────────────────────────────────────────────────────────
    return True

# ══════════════════════════════════════════════════════════════
#  SCORING TECHNICZNY
# ══════════════════════════════════════════════════════════════

def calc_tech_score(sig: dict) -> int:
    """
    Scoring techniczny 0–10.
    Wyższy wynik = silniejszy sygnał wejścia.
    """
    score = 0
    # Typ sygnału (maks 3 pkt)
    signal_type = sig.get("signal", "")
    if signal_type == "Strong BUY":
        score += 3
    elif signal_type == "BUY":
        score += 1
    # Strefa tygodniowego SMI (maks 2 pkt)
    zone = sig.get("zone", "")
    if zone == "OVERSOLD":
        score += 2
    elif zone == "Bearish":
        score += 1
    # Dywergencja bycza (+2 pkt — rzadka, bardzo wartościowa)
    if sig.get("divergence_bull"):
        score += 2
    # Fundamenty (+2 pkt łącznie)
    eps = sig.get("eps_ttm") or 0
    qr  = sig.get("quick_ratio") or 0
    if eps > 0:
        score += 1
    if qr >= 1.0:
        score += 1
    # ROIC i Gross Margin jako bonus (+1 pkt łącznie)
    roic = sig.get("roic")
    gm   = sig.get("gross_margin")
    if roic is not None and roic >= MIN_ROIC:
        score += 1
    return min(score, 10)


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
  .nav-link-active{color:#fff;border-color:var(--accent);background:var(--bg3);
                   pointer-events:none}

  .stats-bar{display:flex;flex-wrap:wrap;gap:.75rem;margin:1.5rem 0}
  .stat{background:var(--bg2);border:1px solid var(--border);border-radius:8px;
        padding:.6rem 1.1rem;min-width:110px}
  .stat-val{font-size:1.4rem;font-weight:700;color:#fff}
  .stat-val.green{color:var(--green)} .stat-val.orange{color:var(--orange)}
  .stat-val.blue{color:var(--accent)} .stat-val.purple{color:var(--purple)}
  .stat-label{font-size:.72rem;color:var(--muted);margin-top:.1rem}

  .info-box{border-radius:10px;padding:.9rem 1.4rem;margin-bottom:1.5rem;
            font-size:.82rem;line-height:1.7}
  .info-box ul{margin:.4rem 0 0 1.2rem}

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

  .badge-score-high  {background:#0b2318;color:#3ecf8e;font-size:.68rem;font-weight:700;
                      padding:.15rem .45rem;border-radius:3px;margin-left:.3rem}
  .badge-score-mid   {background:#0d1a2e;color:#7c9ef0;font-size:.68rem;font-weight:700;
                      padding:.15rem .45rem;border-radius:3px;margin-left:.3rem}
  .badge-score-low   {background:#1a1505;color:#ffa040;font-size:.68rem;font-weight:700;
                      padding:.15rem .45rem;border-radius:3px;margin-left:.3rem}
  .badge-div         {background:#1a0a2e;color:#c471ed;font-size:.68rem;font-weight:700;
                      padding:.15rem .45rem;border-radius:3px;margin-left:.3rem;
                      title:"Dywergencja bycza"}

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
    if score >= 7:
        cls = "badge-score-high"
    elif score >= 4:
        cls = "badge-score-mid"
    else:
        cls = "badge-score-low"
    return f'<span class="{cls}" title="Scoring techniczny">&#9733;{score}</span>'

def _div_badge(has_div, desc=""):
    if not has_div: return ""
    title = desc or "Dywergencja bycza"
    return f'<span class="badge-div" title="{title}">DIV</span>'

def _color_ok(val, ok):
    """Zwraca kolor zielony/czerwony/szary w zależności czy warunek ok jest spełniony."""
    if val is None: return "#888"
    return "#3ecf8e" if ok else "#ff4560"

def render_cards(data, show_quality=False):
    """
    show_quality=True  → dodaje sekcję ROIC / D/E / Gross Margin (screener główny)
    show_quality=False → pomija (full scan)
    """
    if not data:
        return "<div class='empty'>Brak sygnalow</div>"
    cards = ""
    for r in sorted(data, key=lambda x: (
        {"Strong BUY":0,"BUY":1,"Turning Up":2}.get(x["signal"],9),
        -(x.get("tech_score") or 0)
    )):
        sig = r.get("signal","Strong BUY")
        tc, sl, sc = _signal_cfg(sig)
        mc = "usa" if r["market"] == "USA" else "eu"
        z  = r.get("zone","--")
        zc = _zone_color(z)

        disc = r.get("discount_52w")
        disc_row = ""
        if disc is not None:
            dc = "#00e599" if disc >= 50 else "#ffb800" if disc >= 30 else "#888"
            disc_row = (f'<div class="sc-row"><span>Discount 52W</span>'
                        f'<span style="color:{dc};font-weight:600">-{disc}%</span></div>')

        eps   = r.get("eps_ttm")
        qr    = r.get("quick_ratio")
        eps_c = _color_ok(eps,  eps  is not None and eps  > 0)
        qr_c  = _color_ok(qr,   qr   is not None and qr   >= 1.0)

        # Nowe metryki
        roic  = r.get("roic")
        de    = r.get("debt_equity")
        gm    = r.get("gross_margin")
        roic_c = _color_ok(roic, roic is not None and roic >= MIN_ROIC)
        de_c   = _color_ok(de,   de   is not None and de   <  MAX_DEBT_EQUITY)
        gm_c   = _color_ok(gm,   gm   is not None and gm   >= MIN_GROSS_MARGIN)

        quality_rows = ""
        if show_quality:
            quality_rows = (
                f'<div class="sc-divider"></div>'
                f'<div class="sc-row"><span>ROIC</span>'
                f'<span style="color:{roic_c};font-weight:600">{fmt_pct(roic)}</span></div>'
                f'<div class="sc-row"><span>Debt / Equity</span>'
                f'<span style="color:{de_c};font-weight:600">{na(de)}</span></div>'
                f'<div class="sc-row"><span>Gross Margin</span>'
                f'<span style="color:{gm_c};font-weight:600">{fmt_pct(gm)}</span></div>'
            )

        cards += (
            f'<div class="signal-card">'
            f'<div style="position:absolute;top:0;left:0;right:0;height:3px;background:{tc}"></div>'
            f'<div style="display:flex;justify-content:space-between;align-items:flex-start">'
            f'<div><div class="sc-ticker">{r["ticker"]}'
            f'{_score_badge(r.get("tech_score"))}'
            f'{_div_badge(r.get("divergence_bull"), r.get("divergence_desc",""))}'
            f'</div>'
            f'<div class="sc-name">{r["name"]}</div></div>'
            f'<span class="badge-{mc}">{r["market"]}</span></div>'
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
            f'<span style="color:{eps_c}">{na(eps)}</span></div>'
            f'<div class="sc-row"><span>Sales TTM</span>'
            f'<span>{na(r.get("sales_ttm_mln"))} M</span></div>'
            f'<div class="sc-row"><span>Quick Ratio</span>'
            f'<span style="color:{qr_c}">{na(qr)}</span></div>'
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
        return "<tr><td colspan='17' style='text-align:center;color:#888;padding:2rem'>Brak wynikow</td></tr>"
    data = sorted(data, key=lambda x: (
        {"Strong BUY":0,"BUY":1,"Turning Up":2}.get(x["signal"],9),
        -(x.get("tech_score") or 0)
    ))
    html = ""
    for r in data:
        disc = r.get("discount_52w")
        disc_str   = f"-{disc}%" if disc is not None else "--"
        disc_color = "#00e599" if (disc or 0) >= 50 else "#ffb800" if (disc or 0) >= 30 else "#888"
        sig = r["signal"]
        badge = ('<span class="badge-strong">STRONG</span>' if sig == "Strong BUY"
                 else '<span class="badge-buy">BUY</span>'    if sig == "BUY"
                 else '<span class="badge-turning">TURN</span>')

        eps  = r.get("eps_ttm");  qr = r.get("quick_ratio")
        roic = r.get("roic");     de = r.get("debt_equity")
        gm   = r.get("gross_margin")

        eps_c  = "#3ecf8e" if eps  and eps  > 0    else ("#ff4560" if eps  is not None else "inherit")
        qr_c   = "#3ecf8e" if qr   and qr   >= 1.0 else ("#ff4560" if qr   is not None else "inherit")
        roic_c = "#3ecf8e" if roic and roic >= MIN_ROIC        else ("#ff4560" if roic is not None else "inherit")
        de_c   = "#3ecf8e" if de   is not None and de < MAX_DEBT_EQUITY  else ("#ff4560" if de   is not None else "inherit")
        gm_c   = "#3ecf8e" if gm   and gm   >= MIN_GROSS_MARGIN else ("#ff4560" if gm   is not None else "inherit")

        quality_cols = ""
        if show_quality:
            quality_cols = (
                f'<td class="num" style="color:{roic_c}">{fmt_pct(roic)}</td>'
                f'<td class="num" style="color:{de_c}">{na(de)}</td>'
                f'<td class="num" style="color:{gm_c}">{fmt_pct(gm)}</td>'
            )

        html += f"""<tr>
          <td><span class="ticker">{r['ticker']}</span>{badge}{_score_badge(r.get('tech_score'))}{_div_badge(r.get('divergence_bull'), r.get('divergence_desc',''))}</td>
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
          <td class="num">{na(r.get('sales_ttm_mln'))} M</td>
          <td class="num" style="color:{qr_c}">{na(qr)}</td>
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

    html = f"""<!DOCTYPE html>
<html lang="pl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Stock Screener SMI – {dt}</title>
<style>
{COMMON_CSS}
  .strategy-box{{background:var(--bg2);border:1px solid #ff6b00;border-radius:10px;
                padding:1rem 1.4rem;font-size:.82rem;line-height:1.7}}
  .strategy-box strong{{color:var(--orange)}}
  .strategy-box ul{{margin:.4rem 0 0 1.2rem;columns:2;gap:2rem}}
  @media(max-width:600px){{.strategy-box ul{{columns:1}}}}
</style>
</head>
<body>
<div class="page">
  <nav class="report-nav">
    <a href="index.html"     class="nav-link">&#127968; Start</a>
    <a href="screener.html"  class="nav-link nav-link-active">&#9889; Screener g&#322;&#243;wny</a>
    <a href="index_all.html" class="nav-link">&#128270; Full Scan</a>
  </nav>

  <h1>Screener g&#322;&#243;wny &mdash; SMI Tygodniowy</h1>
  <p class="subtitle">Wygenerowano: {dt} &nbsp;|&nbsp; Czas: {meta['elapsed_min']} min &nbsp;|&nbsp; SMI({SMI_LEN_K},{SMI_LEN_D},{SMI_LEN_EMA})</p>

  <div class="strategy-box" style="margin:1.5rem 0">
    <strong>&#9881; Aktywna strategia &mdash; wszystkie warunki muszą być spełnione:</strong>
    <ul>
      <li>Sygnal: <strong>Strong BUY</strong> lub <strong>Turning Up</strong></li>
      <li>Discount &ge; <strong>{int(MIN_DISCOUNT_52W*100)}%</strong> vs 52W High</li>
      <li>EPS TTM &gt; <strong>0</strong></li>
      <li>Quick Ratio &ge; <strong>{MIN_QUICK}</strong></li>
      <li>ROIC &gt; <strong>{int(MIN_ROIC*100)}%</strong></li>
      <li>Debt / Equity &lt; <strong>{MAX_DEBT_EQUITY}</strong></li>
      <li>Gross Margin &gt; <strong>{int(MIN_GROSS_MARGIN*100)}%</strong></li>
      <li>Cap &gt; <strong>{MIN_MARKET_CAP//1_000_000}M</strong> &nbsp;|&nbsp; Vol &gt; <strong>{MIN_VOLUME:,}</strong></li>
    </ul>
  </div>

  <div class="stats-bar">
    <div class="stat"><div class="stat-val">{meta['total_scanned']}</div><div class="stat-label">Przeskanowano</div></div>
    <div class="stat"><div class="stat-val">{meta['weekly_signals']}</div><div class="stat-label">Sygnalow SMI</div></div>
    <div class="stat"><div class="stat-val green">{meta['main_total']}</div><div class="stat-label">Po filtrach</div></div>
    <div class="stat"><div class="stat-val orange">{len(strong_res)}</div><div class="stat-label">Strong BUY</div></div>
    <div class="stat"><div class="stat-val purple">{len(turning_res)}</div><div class="stat-label">Turning Up</div></div>
  </div>

  <div class="section section-strong">
    <div class="section-header"><span class="section-icon">&#9889;</span>
      <h2>Strong BUY &mdash; {len(strong_res)} sygnalow</h2></div>
    <p style="font-size:.8rem;color:var(--muted);margin-bottom:1rem">
      Crossover SMI &gt; EMA ze strefy wyprzedania (&lt;&minus;40)
    </p>
    {render_cards(strong_res, show_quality=True)}
  </div>

  <div class="section section-turning">
    <div class="section-header"><span class="section-icon">&#128260;</span>
      <h2>Turning Up &mdash; {len(turning_res)} sygnalow</h2></div>
    <p style="font-size:.8rem;color:var(--muted);margin-bottom:1rem">
      SMI osiagnal lokalny dolek, zmienia kierunek &mdash; jeszcze ponizej EMA
    </p>
    {render_cards(turning_res, show_quality=True)}
  </div>

  <div class="section">
    <div class="section-header"><span class="section-icon">&#128203;</span>
      <h2>Wszystkie wyniki &mdash; {len(results)}</h2></div>
    <div class="table-wrap">
    <table>
      <thead><tr>
        <th>Ticker</th><th>Nazwa</th><th>Rynek</th><th>Sektor</th>
        <th class="num">Cena</th><th class="num">Discount 52W</th>
        <th class="num">Cap</th><th class="num">Vol avg</th>
        <th class="num">SMI (W)</th><th class="num">EMA (W)</th><th>Strefa</th>
        <th class="num">EPS</th><th class="num">Sales</th><th class="num">QR</th>
        <th class="num">ROIC</th><th class="num">D/E</th><th class="num">Gr.Margin</th>
      </tr></thead>
      <tbody>{render_table_rows(results, show_quality=True)}</tbody>
    </table>
    </div>
  </div>
</div>
</body>
</html>"""

    path = f"{OUTPUT_DIR}/screener.html"
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
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
<html lang="pl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Full Scan SMI – {dt}</title>
<style>
{COMMON_CSS}
  .info-box{{background:var(--bg2);border:1px solid #3ecf8e44}}
  .info-box strong{{color:var(--green)}}
</style>
</head>
<body>
<div class="page">
  <nav class="report-nav">
    <a href="index.html"     class="nav-link">&#127968; Start</a>
    <a href="screener.html"  class="nav-link">&#9889; Screener g&#322;&#243;wny</a>
    <a href="index_all.html" class="nav-link nav-link-active">&#128270; Full Scan</a>
  </nav>

  <h1>Full Scan &mdash; SMI Tygodniowy <span style="font-size:.85rem;color:var(--green);font-weight:400">[bez filtrow fundamentalnych]</span></h1>
  <p class="subtitle">Wygenerowano: {dt} &nbsp;|&nbsp; Czas: {meta['elapsed_min']} min &nbsp;|&nbsp; SMI({SMI_LEN_K},{SMI_LEN_D},{SMI_LEN_EMA})</p>

  <div class="info-box" style="margin:1.5rem 0;padding:.9rem 1.4rem;font-size:.82rem;line-height:1.7;border-radius:10px">
    <strong>&#128270; Wszystkie sygnaly SMI</strong> (Strong BUY / BUY / Turning Up).
    Filtr tylko: <strong>Cap &gt; {MIN_MARKET_CAP//1_000_000}M</strong> i <strong>Vol &gt; {MIN_VOLUME:,}</strong>.
    Pozostale metryki wyswietlane informacyjnie.
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
    <p style="font-size:.8rem;color:var(--muted);margin-bottom:1rem">
      Crossover SMI &gt; EMA ze strefy neutralnej lub byczej
    </p>
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
    <div class="table-wrap">
    <table>
      <thead><tr>
        <th>Ticker</th><th>Nazwa</th><th>Rynek</th><th>Sektor</th>
        <th class="num">Cena</th><th class="num">Discount 52W</th>
        <th class="num">Cap</th><th class="num">Vol avg</th>
        <th class="num">SMI (W)</th><th class="num">EMA (W)</th><th>Strefa</th>
        <th class="num">EPS*</th><th class="num">Sales</th><th class="num">QR*</th>
      </tr></thead>
      <tbody>{render_table_rows(results, show_quality=False)}</tbody>
    </table>
    </div>
    <p style="font-size:.72rem;color:var(--muted);margin-top:.7rem">
      * dane informacyjne, nie filtruja wynikow w Full Scan.
    </p>
  </div>
</div>
</body>
</html>"""

    path = f"{OUTPUT_DIR}/index_all.html"
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  Raport full scan: {path}")

# ══════════════════════════════════════════════════════════════
#  HTML – STRONA STARTOWA
# ══════════════════════════════════════════════════════════════

def generate_html_index(meta):
    dt         = datetime.fromisoformat(meta["generated_at"]).strftime("%d.%m.%Y %H:%M")
    main_count = meta.get("main_total", 0)
    full_count = meta.get("full_total", 0)

    html = f"""<!DOCTYPE html>
<html lang="pl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Stock Screener SMI</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;700;800&display=swap');
  :root {{
    --bg:#080a14; --bg2:#0e1020; --border:#1c2040;
    --text:#c8cde8; --muted:#454a6a; --accent:#7c9ef0;
    --green:#3ecf8e; --orange:#ff6b00; --yellow:#ffb800; --purple:#c471ed;
  }}
  *,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:'Syne',sans-serif;background:var(--bg);color:var(--text);
       min-height:100vh;display:flex;flex-direction:column;
       align-items:center;justify-content:center;padding:2rem;overflow-x:hidden}}
  body::before{{content:'';position:fixed;inset:0;
    background-image:linear-gradient(rgba(124,158,240,.04) 1px,transparent 1px),
                     linear-gradient(90deg,rgba(124,158,240,.04) 1px,transparent 1px);
    background-size:40px 40px;pointer-events:none;z-index:0}}
  .blob{{position:fixed;border-radius:50%;filter:blur(80px);pointer-events:none;z-index:0;
         animation:drift 12s ease-in-out infinite alternate}}
  .blob-1{{width:380px;height:380px;background:rgba(255,107,0,.08);top:-80px;right:-60px}}
  .blob-2{{width:300px;height:300px;background:rgba(62,207,142,.06);bottom:-60px;left:-40px;animation-delay:-5s}}
  .blob-3{{width:200px;height:200px;background:rgba(124,158,240,.07);top:50%;left:50%;
           transform:translate(-50%,-50%);animation-delay:-9s}}
  @keyframes drift{{from{{transform:translate(0,0) scale(1)}}to{{transform:translate(20px,15px) scale(1.08)}}}}
  .wrapper{{position:relative;z-index:1;text-align:center;max-width:700px;width:100%}}
  .badge{{display:inline-flex;align-items:center;gap:.45rem;
          background:rgba(124,158,240,.08);border:1px solid rgba(124,158,240,.2);
          border-radius:20px;padding:.3rem .9rem;font-family:'Space Mono',monospace;
          font-size:.72rem;color:var(--accent);letter-spacing:.04em;margin-bottom:1.6rem;
          animation:fadein .6s ease both}}
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
  .cards{{display:grid;grid-template-columns:1fr 1fr;gap:1.2rem;
          margin-bottom:2.5rem;animation:fadein .6s .3s ease both}}
  .card{{position:relative;background:var(--bg2);border:1px solid var(--border);
         border-radius:16px;padding:1.8rem 1.5rem 1.6rem;text-decoration:none;
         color:var(--text);overflow:hidden;transition:transform .2s,border-color .2s,box-shadow .2s;text-align:left}}
  .card::before{{content:'';position:absolute;inset:0;opacity:0;transition:opacity .25s;border-radius:16px}}
  .card:hover{{transform:translateY(-4px)}}
  .card-main::before{{background:radial-gradient(circle at 30% 20%,rgba(255,107,0,.18),transparent 65%)}}
  .card-full::before{{background:radial-gradient(circle at 30% 20%,rgba(62,207,142,.12),transparent 65%)}}
  .card:hover::before{{opacity:1}}
  .card-main{{border-color:rgba(255,107,0,.25)}}
  .card-full{{border-color:rgba(62,207,142,.2)}}
  .card-main:hover{{border-color:var(--orange);box-shadow:0 8px 32px rgba(255,107,0,.12)}}
  .card-full:hover{{border-color:var(--green);box-shadow:0 8px 32px rgba(62,207,142,.1)}}
  .card-bar{{position:absolute;top:0;left:0;right:0;height:3px;border-radius:16px 16px 0 0}}
  .card-main .card-bar{{background:linear-gradient(90deg,var(--orange),var(--yellow))}}
  .card-full .card-bar{{background:linear-gradient(90deg,var(--green),var(--accent))}}
  .card-icon{{font-size:1.8rem;margin-bottom:.9rem;display:block}}
  .card-title{{font-size:1.15rem;font-weight:700;color:#fff;margin-bottom:.35rem}}
  .card-desc{{font-size:.8rem;color:var(--muted);line-height:1.55;margin-bottom:.8rem}}
  .card-count{{font-family:'Space Mono',monospace;font-size:.78rem;font-weight:700;margin-bottom:.9rem}}
  .card-main .card-count{{color:var(--orange)}}
  .card-full .card-count{{color:var(--green)}}
  .card-tags{{display:flex;flex-wrap:wrap;gap:.4rem}}
  .tag{{font-family:'Space Mono',monospace;font-size:.65rem;padding:.2rem .55rem;
        border-radius:4px;font-weight:700;letter-spacing:.03em}}
  .tag-orange{{background:rgba(255,107,0,.12);color:var(--orange)}}
  .tag-green {{background:rgba(62,207,142,.1);color:var(--green)}}
  .tag-blue  {{background:rgba(124,158,240,.1);color:var(--accent)}}
  .tag-purple{{background:rgba(196,113,237,.1);color:var(--purple)}}
  .card-arrow{{position:absolute;bottom:1.4rem;right:1.4rem;font-size:1rem;
               color:var(--muted);transition:color .2s,transform .2s}}
  .card:hover .card-arrow{{color:#fff;transform:translate(3px,-3px)}}
  .info-bar{{display:flex;justify-content:center;gap:2rem;flex-wrap:wrap;
             animation:fadein .6s .45s ease both}}
  .info-item{{display:flex;align-items:center;gap:.5rem;font-family:'Space Mono',monospace;
              font-size:.72rem;color:var(--muted)}}
  .info-item span:first-child{{color:var(--accent)}}
  @keyframes fadein{{from{{opacity:0;transform:translateY(14px)}}to{{opacity:1;transform:translateY(0)}}}}
  @media(max-width:560px){{.cards{{grid-template-columns:1fr}}h1{{font-size:2rem}}}}
</style>
</head>
<body>
<div class="blob blob-1"></div>
<div class="blob blob-2"></div>
<div class="blob blob-3"></div>
<div class="wrapper">
  <div class="badge"><div class="badge-dot"></div>SMI(10,3,3) &nbsp;&middot;&nbsp; Interwal tygodniowy</div>
  <h1>Stock<br><span>Screener</span></h1>
  <p class="sub">Skanuje rynki USA i EU w poszukiwaniu sygnalow<br>
     wskaznika <code>Stochastic Momentum Index</code><br>
     <span style="font-size:.8rem">Ostatni skan: {dt}</span></p>
  <div class="cards">
    <a href="screener.html" class="card card-main">
      <div class="card-bar"></div>
      <span class="card-icon">&#9889;</span>
      <div class="card-title">Screener glowny</div>
      <div class="card-desc">Strong BUY i Turning Up z pelnym zestawem filtrow fundamentalnych.</div>
      <div class="card-count">&#9662; {main_count} wynikow</div>
      <div class="card-tags">
        <span class="tag tag-orange">Strong BUY</span>
        <span class="tag tag-purple">Turning Up</span>
        <span class="tag tag-blue">ROIC &gt;15%</span>
        <span class="tag tag-blue">D/E &lt;1</span>
        <span class="tag tag-blue">GM &gt;30%</span>
      </div>
      <div class="card-arrow">&#8599;</div>
    </a>
    <a href="index_all.html" class="card card-full">
      <div class="card-bar"></div>
      <span class="card-icon">&#128270;</span>
      <div class="card-title">Full Scan</div>
      <div class="card-desc">Wszystkie sygnaly SMI bez filtrow fundamentalnych. Tylko plynnosc.</div>
      <div class="card-count">&#9662; {full_count} wynikow</div>
      <div class="card-tags">
        <span class="tag tag-orange">Strong BUY</span>
        <span class="tag tag-green">BUY</span>
        <span class="tag tag-purple">Turning Up</span>
        <span class="tag tag-blue">Cap &gt;200M</span>
      </div>
      <div class="card-arrow">&#8599;</div>
    </a>
  </div>
  <div class="info-bar">
    <div class="info-item"><span>&#9670;</span> S&amp;P 500 + NASDAQ + NYSE + AMEX</div>
    <div class="info-item"><span>&#9670;</span> DAX &middot; CAC &middot; FTSE &middot; AEX &middot; WIG + inne</div>
    <div class="info-item"><span>&#9670;</span> Aktualizacja: GitHub Actions</div>
  </div>
</div>
</body>
</html>"""

    path = f"{OUTPUT_DIR}/index.html"
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  Strona startowa: {path}")

# ══════════════════════════════════════════════════════════════
#  LISTY TRADINGVIEW
# ══════════════════════════════════════════════════════════════

# Mapowanie sufiksów Yahoo Finance → prefiksy giełd TradingView
_TV_SUFFIX_MAP = {
    ".DE":  "XETR",      # Deutsche Börse XETRA
    ".PA":  "EURONEXT",  # Euronext Paris
    ".L":   "LSE",       # London Stock Exchange
    ".AS":  "EURONEXT",  # Euronext Amsterdam
    ".MC":  "BME",       # Bolsa de Madrid
    ".SW":  "SIX",       # SIX Swiss Exchange
    ".MI":  "MIL",       # Borsa Italiana
    ".ST":  "OM",        # Nasdaq Stockholm
    ".OL":  "OSL",       # Oslo Børs
    ".BR":  "EURONEXT",  # Euronext Brussels
    ".WA":  "GPW",       # Giełda Papierów Wartościowych Warszawa
}

def _to_tv_ticker(yahoo_ticker: str) -> str:
    """Konwertuje ticker Yahoo Finance na format TradingView (EXCHANGE:TICKER)."""
    for suffix, exchange in _TV_SUFFIX_MAP.items():
        if yahoo_ticker.endswith(suffix):
            base = yahoo_ticker[: -len(suffix)]
            return f"{exchange}:{base}"
    # Brak sufiksu → ticker US, domyślna giełda rozpoznawana przez TV
    return yahoo_ticker


def generate_tradingview_lists(main_results: list, full_results: list) -> None:
    """
    Generuje dwa pliki watchlist dla TradingView:
      results/tv_main.txt  – Screener główny (wszystkie filtry)
      results/tv_all.txt   – Full Scan (tylko płynność)

    Format pliku: jeden ticker na linię, EXCHANGE:TICKER
    Import w TradingView: Watchlist → ⋮ → Import list
    """
    dt_str = datetime.now().strftime("%Y-%m-%d %H:%M UTC")

    for label, data, filename in [
        ("Screener główny", main_results, "tv_main.txt"),
        ("Full Scan",       full_results, "tv_all.txt"),
    ]:
        # Sortuj: Strong BUY pierwsze, potem BUY, Turning Up; w ramach grupy wg discount
        sorted_data = sorted(data, key=lambda x: (
            {"Strong BUY": 0, "BUY": 1, "Turning Up": 2}.get(x["signal"], 9),
            -(x.get("discount_52w") or 0),
        ))

        lines = [
            f"### TradingView Watchlist – {label}",
            f"### Wygenerowano: {dt_str}",
            f"### Liczba tickerow: {len(sorted_data)}",
            f"### Format: EXCHANGE:TICKER  |  sygnał  |  discount 52W  |  sektor",
            "###",
        ]

        # Grupuj według sygnału
        for sig_type in ("Strong BUY", "BUY", "Turning Up"):
            group = [r for r in sorted_data if r["signal"] == sig_type]
            if not group:
                continue
            lines.append(f"### ── {sig_type} ({len(group)}) ──")
            for r in group:
                tv     = _to_tv_ticker(r["ticker"])
                disc   = f"-{r['discount_52w']}%" if r.get("discount_52w") is not None else "--"
                sector = r.get("sector", "--")
                lines.append(f"{tv}  ### {r['signal']} | {disc} | {sector}")

        path = f"{OUTPUT_DIR}/{filename}"
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")

        # Wersja "czysta" – sam ticker, bez komentarzy (dla starszych wersji TV)
        clean_lines = [_to_tv_ticker(r["ticker"]) for r in sorted_data]
        clean_path  = path.replace(".txt", "_clean.txt")
        with open(clean_path, "w", encoding="utf-8") as f:
            f.write("\n".join(clean_lines) + "\n")

        print(f"  TradingView {label}: {path}  ({len(sorted_data)} tickerow)")
        print(f"  TradingView {label} (clean): {clean_path}")


# ══════════════════════════════════════════════════════════════
#  GŁÓWNA PĘTLA
# ══════════════════════════════════════════════════════════════

def run_screener():
    t0 = datetime.now()
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("=" * 60)
    print(f"SCREENER START: {t0.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"SMI({SMI_LEN_K},{SMI_LEN_D},{SMI_LEN_EMA}) | sygnal tygodniowy")
    print(f"Generuje: index.html + screener.html + index_all.html")
    print("=" * 60)

    print("\n[Tickery] Pobieranie list spolek...")
    usa = list(set(get_sp500() + get_nasdaq() + get_nyse_amex()))
    eu  = list(set(get_european_indices()))
    ticker_market = [(t, "USA") for t in usa] + [(t, "EU") for t in eu]
    print(f"\nLacznie: {len(ticker_market)} ({len(usa)} USA, {len(eu)} EU)")

    weekly_signals = phase1_weekly_signals(ticker_market)
    all_data       = phase2_collect(weekly_signals)

    main_results = [r for r in all_data if filter_main(r)]
    full_results = all_data

    elapsed = round((datetime.now() - t0).total_seconds() / 60, 1)

    meta = {
        "generated_at":  datetime.now().isoformat(),
        "elapsed_min":   elapsed,
        "total_scanned": len(ticker_market),
        "weekly_signals":len(weekly_signals),
        "main_total":    len(main_results),
        "full_total":    len(full_results),
        "indicator":     f"SMI({SMI_LEN_K},{SMI_LEN_D},{SMI_LEN_EMA})",
    }

    for fname, data in [
        ("results",      full_results),
        ("results_main", main_results),
        ("meta",         meta),
    ]:
        with open(f"{OUTPUT_DIR}/{fname}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    if full_results:
        pd.DataFrame(full_results).to_csv(f"{OUTPUT_DIR}/results.csv", index=False)
    if main_results:
        pd.DataFrame(main_results).to_csv(f"{OUTPUT_DIR}/results_main.csv", index=False)

    print("\n[HTML] Generowanie raportow...")
    generate_html_main(meta, main_results)
    generate_html_full(meta, full_results)
    generate_html_index(meta)

    print("\n[TV] Listy TradingView...")
    generate_tradingview_lists(main_results, full_results)

    print(f"\nCzas lacznie: {elapsed} min")
    print(f"Screener glowny : {len(main_results)} wynikow")
    print(f"Full Scan       : {len(full_results)} wynikow")
    print(f"Wyniki w: {OUTPUT_DIR}/")
    return main_results, full_results


if __name__ == "__main__":
    run_screener()
