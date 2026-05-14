"""
Stock Screener – SMI Tygodniowy
Trzy typy sygnałów (tygodniowy interwał):
  ⚡ Strong BUY  — crossover SMI > EMA ze strefy wyprzedania (< −40)
  ✅ BUY         — crossover SMI > EMA (strefa neutralna lub bycza)
  🔄 Turning Up  — SMI osiągnął lokalny dołek i zmienia kierunek na rosnący,
                   ale jeszcze nie przekroczyło EMA (wczesny sygnał)

Fundamenty: market cap > 200M | volume > 300K | EPS TTM > 0 | Sales > 0 | QR > 1.0
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
MIN_MARKET_CAP       = 200_000_000
MIN_VOLUME           = 300_000
MIN_QUICK            = 1.0
FUNDAMENTALS_WORKERS = 20
DOWNLOAD_BATCH_SIZE  = 100
OUTPUT_DIR           = "results"
SMI_LEN_K, SMI_LEN_D, SMI_LEN_EMA = 10, 3, 3

# ══════════════════════════════════════════════════════════════
#  POBIERANIE TICKERÓW
# ══════════════════════════════════════════════════════════════

def get_sp500():
    try:
        url = ("https://www.ishares.com/us/products/239726/ISHARES-CORE-SP-500-ETF/"
               "1467271812596.ajax?fileType=csv&fileName=IVV_holdings&dataType=fund")
        r  = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        df = pd.read_csv(StringIO(r.text), skiprows=9)
        df = df[df["Asset Class"] == "Equity"]
        tickers = df["Ticker"].dropna().str.strip().str.replace(".", "-", regex=False).tolist()
        print(f"  S&P 500 (iShares IVV): {len(tickers)} spolki")
        return tickers
    except Exception as e:
        print(f"  S&P 500 blad: {e}")
        return []

def get_sp600():
    try:
        url = ("https://www.ishares.com/us/products/239774/ISHARES-CORE-SP-SMALLCAP-ETF/"
               "1467271812596.ajax?fileType=csv&fileName=IJR_holdings&dataType=fund")
        r  = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        df = pd.read_csv(StringIO(r.text), skiprows=9)
        df = df[df["Asset Class"] == "Equity"]
        tickers = df["Ticker"].dropna().str.strip().str.replace(".", "-", regex=False).tolist()
        print(f"  S&P 600 (iShares IJR): {len(tickers)} spolki")
        return tickers
    except Exception as e:
        print(f"  S&P 600 blad: {e}")
        return []

def get_russell2000():
    try:
        url = ("https://www.ishares.com/us/products/239710/ISHARES-RUSSELL-2000-ETF/"
               "1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund")
        r  = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        df = pd.read_csv(StringIO(r.text), skiprows=9)
        df = df[df["Asset Class"] == "Equity"]
        tickers = df["Ticker"].dropna().str.strip().tolist()
        print(f"  Russell 2000: {len(tickers)} spolki")
        return tickers
    except Exception as e:
        print(f"  Russell 2000 blad: {e}")
        return []

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
    print(f"  EU statyczna lista: {len(all_eu)} tickerow")
    print(f"    DAX:{len(dax)} CAC:{len(cac)} FTSE:{len(ftse)} AEX:{len(aex)} "
          f"IBEX:{len(ibex)} SMI:{len(smi_idx)} MIB:{len(mib)} "
          f"OMX:{len(omx)} OBX:{len(obx)} BEL:{len(bel)} WIG:{len(wig)}")
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

def smi_weekly_signal(smi, smi_ema):
    """
    Klasyfikuje tygodniowy sygnał SMI:

      "Strong BUY"  -- crossover SMI > EMA ze strefy wyprzedania (poprzedni bar <= -40)
      "BUY"         -- crossover SMI > EMA (strefa neutralna lub bycza)
      "Turning Up"  -- SMI osiagnal lokalny dolek i zmienia kierunek na rosnacy,
                       ale jeszcze ponizej EMA (wczesny sygnal)
      None          -- brak sygnalu

    Zwraca: (signal_type, smi_val, smi_ema_val, zone)
    """
    if len(smi) < 4:
        return None, None, None, "--"

    s0 = float(smi.iloc[-1])
    s1 = float(smi.iloc[-2])
    s2 = float(smi.iloc[-3])
    e0 = float(smi_ema.iloc[-1])
    e1 = float(smi_ema.iloc[-2])

    if any(np.isnan(v) for v in [s0, s1, s2, e0, e1]):
        return None, None, None, "--"

    # Strefa
    if   s0 >= 40:  zone = "OVERBOUGHT"
    elif s0 <= -40: zone = "OVERSOLD"
    elif s0 > 0:    zone = "Bullish"
    else:           zone = "Bearish"

    # Crossover SMI > EMA
    cross_up = (s1 < e1) and (s0 >= e0)
    exit_os  = (s1 < -40) and (s0 >= -40)
    buy      = cross_up or exit_os

    if buy:
        strong = (s1 <= -40) or (s0 <= -40)
        sig    = "Strong BUY" if strong else "BUY"
        return sig, round(s0, 2), round(e0, 2), zone

    # Turning Up: lokalny dolek SMI, jeszcze ponizej EMA
    # s2 >= s1 (dolek na barze -2) i s0 > s1 (teraz rosnie) i s0 < e0 (przed crossoverem)
    turning_up = (s2 >= s1) and (s0 > s1) and (s0 < e0)
    if turning_up:
        return "Turning Up", round(s0, 2), round(e0, 2), zone

    return None, round(s0, 2), round(e0, 2), zone

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
#  FAZA 1 - Tygodniowy sygnal SMI
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
                signals[ticker] = {
                    "market":  market_map[ticker],
                    "smi":     s_val,
                    "smi_ema": e_val,
                    "zone":    zone,
                    "signal":  sig,
                }
        except Exception:
            pass

    s  = sum(1 for v in signals.values() if v["signal"] == "Strong BUY")
    b  = sum(1 for v in signals.values() if v["signal"] == "BUY")
    tu = sum(1 for v in signals.values() if v["signal"] == "Turning Up")
    print(f"      Sygnaly: {len(signals)}  "
          f"( Strong BUY:{s}  BUY:{b}  Turning Up:{tu} )")
    return signals

# ══════════════════════════════════════════════════════════════
#  FAZA 2 - Meta + Fundamenty (rownolegly)
# ══════════════════════════════════════════════════════════════

def _get_fundamentals_from_statements(tkr):
    result = {
        "eps_ttm": None, "sales_ttm_mln": None,
        "quick_ratio": None, "name": None,
        "sector": None, "country": None,
    }
    try:
        fin = tkr.financials
        if fin is not None and not fin.empty:
            for label in ["Total Revenue", "Operating Revenue"]:
                if label in fin.index:
                    rev = fin.loc[label].dropna()
                    if len(rev) >= 1:
                        result["sales_ttm_mln"] = round(float(rev.iloc[0]) / 1e6, 1)
                    break
            for label in ["Net Income", "Net Income Common Stockholders"]:
                if label in fin.index:
                    ni = fin.loc[label].dropna()
                    if len(ni) >= 1:
                        result["eps_ttm"] = round(float(ni.iloc[0]) / 1e6, 2)
                    break
    except Exception:
        pass

    try:
        bs = tkr.balance_sheet
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
                result["quick_ratio"] = round((ca - inv) / cl, 2)
    except Exception:
        pass

    try:
        info = tkr.info
        if info and len(info) > 5:
            result["name"]    = info.get("shortName")
            result["sector"]  = info.get("sector")
            result["country"] = info.get("country")
            eps = info.get("trailingEps")
            if eps is not None:
                result["eps_ttm"] = round(float(eps), 2)
    except Exception:
        pass

    return result


def _check_one(symbol, weekly_data):
    try:
        tkr = yf.Ticker(symbol)
        fi  = tkr.fast_info

        price = getattr(fi, "last_price", None)
        if not price or price <= 0:
            return None

        cap = getattr(fi, "market_cap", None)
        if not cap or cap < MIN_MARKET_CAP:
            return None

        vol = (getattr(fi, "three_month_average_volume", None)
               or getattr(fi, "last_volume", None))
        if not vol or vol < MIN_VOLUME:
            return None

        currency = getattr(fi, "currency", "USD")
        fund     = _get_fundamentals_from_statements(tkr)

        if fund["eps_ttm"] is not None and fund["eps_ttm"] < 0:
            return None
        if fund["quick_ratio"] is not None and fund["quick_ratio"] < MIN_QUICK:
            return None

        return {
            "ticker":         symbol,
            "name":           fund["name"]    or symbol,
            "market":         weekly_data["market"],
            "country":        fund["country"] or "--",
            "sector":         fund["sector"]  or "--",
            "price":          round(price, 2),
            "currency":       currency,
            "market_cap_mln": round(cap / 1e6, 1),
            "volume_k":       round(vol / 1000, 1),
            "smi":            weekly_data["smi"],
            "smi_ema":        weekly_data["smi_ema"],
            "zone":           weekly_data["zone"],
            "signal":         weekly_data["signal"],
            "eps_ttm":        fund["eps_ttm"],
            "sales_ttm_mln":  fund["sales_ttm_mln"],
            "quick_ratio":    fund["quick_ratio"],
            "scanned_at":     datetime.now().isoformat(),
        }
    except Exception:
        return None


def phase2_meta_fundamentals(weekly_signals):
    if not weekly_signals:
        return []
    candidates = list(weekly_signals.keys())
    print(f"\n[2/2] Meta + fundamenty -- {len(candidates)} tickerow "
          f"({FUNDAMENTALS_WORKERS} watkow)...")

    results = []
    with ThreadPoolExecutor(max_workers=FUNDAMENTALS_WORKERS) as pool:
        futures = {
            pool.submit(_check_one, sym, weekly_signals[sym]): sym
            for sym in candidates
        }
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)

    s  = sum(1 for r in results if r["signal"] == "Strong BUY")
    b  = sum(1 for r in results if r["signal"] == "BUY")
    tu = sum(1 for r in results if r["signal"] == "Turning Up")
    print(f"      Spelnia fundamenty: {len(results)}  "
          f"( Strong BUY:{s}  BUY:{b}  Turning Up:{tu} )")
    return results

# ══════════════════════════════════════════════════════════════
#  FORMATOWANIE
# ══════════════════════════════════════════════════════════════

def fmt_cap(mln):
    if mln is None: return "--"
    return f"{mln/1000:.1f} B" if mln >= 1000 else f"{mln:.0f} M"

def fmt_vol(k):
    if k is None: return "--"
    return f"{k/1000:.1f}M" if k >= 1000 else f"{k:.0f}K"

def na(v, suffix=""):
    return f"{v}{suffix}" if v is not None else "--"

# ══════════════════════════════════════════════════════════════
#  RAPORT HTML
# ══════════════════════════════════════════════════════════════

def generate_html(meta, results):
    dt = datetime.fromisoformat(meta["generated_at"]).strftime("%d.%m.%Y %H:%M")

    def zone_badge(zone):
        cls = {"OVERBOUGHT":"zone-ob","OVERSOLD":"zone-os",
               "Bullish":"zone-bull","Bearish":"zone-bear"}.get(zone, "")
        return f'<span class="zone-badge {cls}">{zone}</span>'

    def signal_cards(data, sig_type):
        if not data:
            return "<div class='empty'>Brak sygnalow w tym skanie</div>"
        cards = ""
        for r in sorted(data, key=lambda x: -x.get("market_cap_mln", 0)):
            mc = "usa" if r["market"] == "USA" else "eu"
            z  = r.get("zone","--")

            if sig_type == "Strong BUY":
                tc, sl, sc = "linear-gradient(90deg,#ff6b00,#ffb800)", "STRONG BUY", "#ffb800"
            elif sig_type == "BUY":
                tc, sl, sc = "linear-gradient(90deg,#00c8ff,#00e599)", "BUY", "#00c8ff"
            else:
                tc, sl, sc = "linear-gradient(90deg,#7b2ff7,#c471ed)", "TURNING UP", "#c471ed"

            zc = {"OVERBOUGHT":"#ff4560","OVERSOLD":"#00e599",
                  "Bullish":"#4da6ff","Bearish":"#ffa040"}.get(z,"#888")

            # Odleglosc do crossovera (tylko Turning Up)
            gap_row = ""
            if sig_type == "Turning Up" and r["smi"] is not None and r["smi_ema"] is not None:
                gap = round(r["smi_ema"] - r["smi"], 2)
                gap_row = (f'<div class="sc-row"><span>Do crossovera (delta)</span>'
                           f'<span style="color:#c471ed">-{gap}</span></div>')

            cards += (
                f'<div class="signal-card">'
                f'<div style="position:absolute;top:0;left:0;right:0;height:3px;background:{tc}"></div>'
                f'<div style="display:flex;justify-content:space-between;align-items:flex-start">'
                f'<div><div class="sc-ticker">{r["ticker"]}</div>'
                f'<div class="sc-name">{r["name"]}</div></div>'
                f'<span class="badge-{mc}">{r["market"]}</span></div>'
                f'<div class="sc-price">{r["price"]} {r["currency"]}</div>'
                f'<div class="sc-row"><span>Sygnal</span>'
                f'<span style="color:{sc};font-weight:600">{sl}</span></div>'
                f'{gap_row}'
                f'<div class="sc-row"><span>Strefa SMI</span>'
                f'<span style="color:{zc}">{z}</span></div>'
                f'<div class="sc-row"><span>Sektor</span><span>{r["sector"]}</span></div>'
                f'<div class="sc-row"><span>Market Cap</span>'
                f'<span style="color:var(--accent)">{fmt_cap(r.get("market_cap_mln"))}</span></div>'
                f'<div class="sc-row"><span>Wolumen avg</span>'
                f'<span>{fmt_vol(r.get("volume_k"))}</span></div>'
                f'<div class="sc-divider"></div>'
                f'<div class="sc-row"><span>EPS TTM</span>'
                f'<span style="color:var(--green)">{na(r.get("eps_ttm"))}</span></div>'
                f'<div class="sc-row"><span>Sales TTM</span>'
                f'<span style="color:var(--green)">{na(r.get("sales_ttm_mln"))} M</span></div>'
                f'<div class="sc-row"><span>Quick Ratio</span>'
                f'<span style="color:var(--green)">{na(r.get("quick_ratio"))}</span></div>'
                f'<div class="sc-stoch">'
                f'<div class="sc-stoch-item"><div class="sc-stoch-label">SMI tydz.</div>'
                f'<div class="sc-stoch-val green">{r["smi"]}</div></div>'
                f'<div class="sc-stoch-item"><div class="sc-stoch-label">SMI EMA</div>'
                f'<div class="sc-stoch-val">{r["smi_ema"]}</div></div>'
                f'</div></div>'
            )
        return f'<div class="cards-grid">{cards}</div>'

    def table_rows(data):
        if not data:
            return "<tr><td colspan='13' style='text-align:center;color:#888;padding:2rem'>Brak wynikow</td></tr>"
        order = {"Strong BUY": 0, "BUY": 1, "Turning Up": 2}
        data  = sorted(data, key=lambda x: (order.get(x["signal"], 9),
                                            -x.get("market_cap_mln", 0)))
        html = ""
        for r in data:
            sig = r["signal"]
            if sig == "Strong BUY":
                badge = '<span class="badge-strong">STRONG</span>'
            elif sig == "BUY":
                badge = '<span class="badge-signal">BUY</span>'
            else:
                badge = '<span class="badge-turning">TURN</span>'

            gap_cell = ""
            if sig == "Turning Up" and r["smi"] is not None and r["smi_ema"] is not None:
                gap = round(r["smi_ema"] - r["smi"], 2)
                gap_cell = f'<span style="color:#c471ed;font-size:.7rem"> (-{gap})</span>'

            html += f"""<tr>
              <td><span class="ticker">{r['ticker']}</span>{badge}</td>
              <td class="name-col">{r['name']}</td>
              <td><span class="badge-{'usa' if r['market']=='USA' else 'eu'}">{r['market']}</span></td>
              <td>{r['sector']}</td>
              <td class="num">{r['price']} {r['currency']}</td>
              <td class="num">{fmt_cap(r.get('market_cap_mln'))}</td>
              <td class="num">{fmt_vol(r.get('volume_k'))}</td>
              <td class="num smi-col">{r['smi']}{gap_cell}</td>
              <td class="num">{r['smi_ema']}</td>
              <td>{zone_badge(r['zone'])}</td>
              <td class="num">{na(r.get('eps_ttm'))}</td>
              <td class="num">{na(r.get('sales_ttm_mln'))} M</td>
              <td class="num">{na(r.get('quick_ratio'))}</td>
            </tr>"""
        return html

    strong_res  = [r for r in results if r["signal"] == "Strong BUY"]
    buy_res     = [r for r in results if r["signal"] == "BUY"]
    turning_res = [r for r in results if r["signal"] == "Turning Up"]

    html = f"""<!DOCTYPE html>
<html lang="pl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Stock Screener SMI - {dt}</title>
<style>
  :root {{
    --bg:#0b0d1a; --bg2:#11142a; --bg3:#181c35; --border:#252840;
    --text:#d0d4e8; --muted:#555d7a; --accent:#7c9ef0;
    --green:#3ecf8e; --red:#ff4560; --orange:#ff6b00; --yellow:#ffb800;
    --purple:#c471ed;
  }}
  *,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
        background:var(--bg);color:var(--text);min-height:100vh}}
  .page{{max-width:1600px;margin:0 auto;padding:2rem}}
  h1{{font-size:1.5rem;font-weight:700;color:#fff;letter-spacing:-.3px}}
  h2{{font-size:1.05rem;font-weight:600;color:#fff;margin-bottom:1rem}}
  .subtitle{{font-size:.8rem;color:var(--muted);margin-top:.3rem}}

  .stats-bar{{display:flex;flex-wrap:wrap;gap:.75rem;margin:1.5rem 0}}
  .stat{{background:var(--bg2);border:1px solid var(--border);border-radius:8px;
         padding:.6rem 1.1rem;min-width:110px}}
  .stat-val{{font-size:1.4rem;font-weight:700;color:#fff}}
  .stat-val.green{{color:var(--green)}} .stat-val.orange{{color:var(--orange)}}
  .stat-val.yellow{{color:var(--yellow)}} .stat-val.purple{{color:var(--purple)}}
  .stat-label{{font-size:.72rem;color:var(--muted);margin-top:.1rem}}

  .section{{background:var(--bg2);border:1px solid var(--border);border-radius:12px;
            padding:1.5rem;margin-bottom:1.5rem}}
  .section-strong  {{border-color:#ff6b00;box-shadow:0 0 20px rgba(255,107,0,.08)}}
  .section-buy     {{border-color:#00c8ff;box-shadow:0 0 20px rgba(0,200,255,.05)}}
  .section-turning {{border-color:#7b2ff7;box-shadow:0 0 20px rgba(196,113,237,.08)}}
  .section-header{{display:flex;align-items:center;gap:.6rem;margin-bottom:1.2rem}}
  .section-icon{{font-size:1.2rem}}

  .cards-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:1rem}}
  .signal-card{{background:var(--bg3);border:1px solid var(--border);border-radius:10px;
                padding:1.2rem;position:relative;overflow:hidden}}
  .sc-ticker{{font-size:1.1rem;font-weight:700;color:#fff;letter-spacing:-.3px}}
  .sc-name{{font-size:.75rem;color:var(--muted);margin-top:.1rem;
            white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:180px}}
  .sc-price{{font-size:1.3rem;font-weight:700;color:var(--accent);margin:.7rem 0}}
  .sc-row{{display:flex;justify-content:space-between;font-size:.78rem;
           padding:.25rem 0;border-bottom:1px solid var(--border)}}
  .sc-row span:first-child{{color:var(--muted)}}
  .sc-divider{{height:1px;background:var(--border);margin:.5rem 0}}
  .sc-stoch{{display:flex;gap:.5rem;margin-top:.7rem}}
  .sc-stoch-item{{flex:1;background:var(--bg2);border-radius:6px;padding:.4rem .6rem;text-align:center}}
  .sc-stoch-label{{font-size:.65rem;color:var(--muted)}}
  .sc-stoch-val{{font-size:.95rem;font-weight:600;color:var(--text)}}
  .sc-stoch-val.green{{color:var(--green)}}
  .empty{{color:var(--muted);text-align:center;padding:2rem;font-size:.9rem}}

  .table-wrap{{overflow-x:auto}}
  table{{width:100%;border-collapse:collapse;font-size:.8rem}}
  th{{background:var(--bg3);color:var(--muted);font-weight:600;text-align:left;
      padding:.6rem 1rem;border-bottom:1px solid var(--border);white-space:nowrap}}
  td{{padding:.55rem 1rem;border-bottom:1px solid rgba(37,40,64,.6);vertical-align:middle}}
  tr:hover td{{background:rgba(255,255,255,.02)}}
  .num{{text-align:right;font-variant-numeric:tabular-nums}}
  .name-col{{max-width:180px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
  .smi-col{{color:var(--green)}}
  .ticker{{font-weight:600;color:#fff;margin-right:.3rem}}

  .badge-strong  {{background:#3d1500;color:var(--orange);font-size:.68rem;font-weight:700;
                   padding:.15rem .45rem;border-radius:3px;margin-left:.2rem}}
  .badge-signal  {{background:#0b2318;color:var(--green);font-size:.68rem;font-weight:700;
                   padding:.15rem .45rem;border-radius:3px;margin-left:.2rem}}
  .badge-turning {{background:#1a0a2e;color:var(--purple);font-size:.68rem;font-weight:700;
                   padding:.15rem .45rem;border-radius:3px;margin-left:.2rem}}
  .badge-usa{{background:#0d1a2e;color:var(--accent);font-size:.72rem;
              padding:.15rem .5rem;border-radius:4px;border:1px solid var(--border)}}
  .badge-eu {{background:#1a1a0d;color:var(--yellow);font-size:.72rem;
              padding:.15rem .5rem;border-radius:4px;border:1px solid var(--border)}}

  .zone-badge{{font-size:.72rem;padding:.15rem .5rem;border-radius:4px;font-weight:500}}
  .zone-ob  {{background:#3d0010;color:#ff4560}}
  .zone-os  {{background:#0b2318;color:var(--green)}}
  .zone-bull{{background:#0d1a2e;color:var(--accent)}}
  .zone-bear{{background:#1a1505;color:#ffa040}}

  @media(max-width:900px){{.page{{padding:1rem}} th,td{{padding:.45rem .6rem}}}}
</style>
</head>
<body>
<div class="page">
  <h1>Stock Screener &mdash; SMI Tygodniowy</h1>
  <p class="subtitle">Wygenerowano: {dt} &nbsp;|&nbsp; Czas: {meta['elapsed_min']} min &nbsp;|&nbsp; SMI({SMI_LEN_K},{SMI_LEN_D},{SMI_LEN_EMA})</p>

  <div class="stats-bar">
    <div class="stat"><div class="stat-val">{meta['total_scanned']}</div><div class="stat-label">Przeskanowano</div></div>
    <div class="stat"><div class="stat-val">{meta['weekly_signals']}</div><div class="stat-label">Sygnalow SMI</div></div>
    <div class="stat"><div class="stat-val green">{meta['results_total']}</div><div class="stat-label">Po filtrach</div></div>
    <div class="stat"><div class="stat-val orange">{meta['results_strong']}</div><div class="stat-label">Strong BUY</div></div>
    <div class="stat"><div class="stat-val yellow">{meta['results_buy']}</div><div class="stat-label">BUY</div></div>
    <div class="stat"><div class="stat-val purple">{meta['results_turning']}</div><div class="stat-label">Turning Up</div></div>
  </div>

  <div class="section section-strong">
    <div class="section-header"><span class="section-icon">&#9889;</span>
      <h2>Strong BUY &mdash; {len(strong_res)} sygnalow</h2></div>
    <p style="font-size:.8rem;color:var(--muted);margin-bottom:1rem">
      Crossover SMI &gt; EMA ze strefy wyprzedania (&lt;&minus;40)
    </p>
    {signal_cards(strong_res, "Strong BUY")}
  </div>

  <div class="section section-buy">
    <div class="section-header"><span class="section-icon">&#10003;</span>
      <h2>BUY &mdash; {len(buy_res)} sygnalow</h2></div>
    <p style="font-size:.8rem;color:var(--muted);margin-bottom:1rem">
      Crossover SMI &gt; EMA (strefa neutralna lub bycza)
    </p>
    {signal_cards(buy_res, "BUY")}
  </div>

  <div class="section section-turning">
    <div class="section-header"><span class="section-icon">&#128260;</span>
      <h2>Turning Up &mdash; {len(turning_res)} sygnalow</h2></div>
    <p style="font-size:.8rem;color:var(--muted);margin-bottom:1rem">
      SMI osiagnal lokalny dolek i zmienia kierunek na rosnacy &mdash; jeszcze ponizej EMA.
      Wczesny sygnal przed potencjalnym crossoverem.
      <strong style="color:var(--purple)">delta</strong> = odleglosc SMI od EMA (im mniejsza, tym blizej crossovera).
    </p>
    {signal_cards(turning_res, "Turning Up")}
  </div>

  <div class="section">
    <div class="section-header"><span class="section-icon">&#128203;</span>
      <h2>Wszystkie wyniki &mdash; {len(results)}</h2></div>
    <div class="table-wrap">
    <table>
      <thead><tr>
        <th>Ticker</th><th>Nazwa</th><th>Rynek</th><th>Sektor</th>
        <th class="num">Cena</th><th class="num">Cap</th><th class="num">Vol avg</th>
        <th class="num">SMI (W)</th><th class="num">EMA (W)</th><th>Strefa</th>
        <th class="num">EPS</th><th class="num">Sales</th><th class="num">QR</th>
      </tr></thead>
      <tbody>{table_rows(results)}</tbody>
    </table>
    </div>
  </div>

  <p style="font-size:.75rem;color:var(--muted);text-align:center;margin-top:1rem">
    Strong BUY = crossover ze strefy wyprzedania &lt;&minus;40 &nbsp;|&nbsp;
    BUY = crossover SMI &gt; EMA &nbsp;|&nbsp;
    Turning Up = lokalny dolek SMI, kierunek zmienia sie na rosnacy (przed crossoverem)
    <br>Fundamenty: Cap &gt; {MIN_MARKET_CAP//1_000_000}M | Vol &gt; {MIN_VOLUME:,} | EPS&gt;0 | Sales&gt;0 | QR&gt;{MIN_QUICK}
  </p>
</div>
</body>
</html>"""

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = f"{OUTPUT_DIR}/index.html"
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Raport: {path}")

# ══════════════════════════════════════════════════════════════
#  GLOWNA PETLA
# ══════════════════════════════════════════════════════════════

def run_screener():
    t0 = datetime.now()
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("=" * 60)
    print(f"SCREENER START: {t0.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"SMI({SMI_LEN_K},{SMI_LEN_D},{SMI_LEN_EMA}) | sygnal tygodniowy")
    print(f"Cap>{MIN_MARKET_CAP//1_000_000}M | Vol>{MIN_VOLUME:,} | EPS>0 | QR>{MIN_QUICK}")
    print("=" * 60)

    print("\n[Tickery] Pobieranie list spolek...")
    usa = list(set(get_sp500() + get_sp600() + get_russell2000()))
    eu  = list(set(get_european_indices()))
    ticker_market = [(t, "USA") for t in usa] + [(t, "EU") for t in eu]
    print(f"\nLacznie: {len(ticker_market)} ({len(usa)} USA, {len(eu)} EU)")

    weekly_signals = phase1_weekly_signals(ticker_market)
    results        = phase2_meta_fundamentals(weekly_signals)

    elapsed     = round((datetime.now() - t0).total_seconds() / 60, 1)
    strong_res  = [r for r in results if r["signal"] == "Strong BUY"]
    buy_res     = [r for r in results if r["signal"] == "BUY"]
    turning_res = [r for r in results if r["signal"] == "Turning Up"]

    meta = {
        "generated_at":    datetime.now().isoformat(),
        "elapsed_min":     elapsed,
        "total_scanned":   len(ticker_market),
        "weekly_signals":  len(weekly_signals),
        "results_total":   len(results),
        "results_strong":  len(strong_res),
        "results_buy":     len(buy_res),
        "results_turning": len(turning_res),
        "indicator":       f"SMI({SMI_LEN_K},{SMI_LEN_D},{SMI_LEN_EMA})",
    }

    for fname, data in [("meta", meta), ("results", results),
                        ("strong", strong_res), ("buy", buy_res),
                        ("turning", turning_res)]:
        with open(f"{OUTPUT_DIR}/{fname}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    if results:
        pd.DataFrame(results).to_csv(f"{OUTPUT_DIR}/results.csv", index=False)

    print(f"\nCzas: {elapsed} min")
    print(f"Wyniki: {len(results)} | Strong BUY:{len(strong_res)} | "
          f"BUY:{len(buy_res)} | Turning Up:{len(turning_res)}")

    generate_html(meta, results)
    print(f"\nWyniki w: {OUTPUT_DIR}/")
    return results


if __name__ == "__main__":
    run_screener()
