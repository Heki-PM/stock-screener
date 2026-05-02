"""
Stock Screener – SMI Signal Strategy + FVG Analysis
Kryteria: USA + Europa | cena < 50 USD/EUR | rosnące Revenue i Net Income QoQ
Sygnał wejścia: SMI crossover / exit OS na interwale tygodniowym
FVG: Fair Value Gap – detekcja niezapełnionych luk na tygodniowym interwale
(port Pine Script: "SMI Signal Strategy" – lengthK=10, lengthD=3, lengthEMA=3)
"""

import yfinance as yf
import pandas as pd
import numpy as np
import requests
import time
import json
import os
from datetime import datetime
from io import StringIO

MAX_PRICE  = 100
DELAY      = 0.25
OUTPUT_DIR = "results"

# FVG – ile tygodniowych świec wstecz szukamy luk
FVG_LOOKBACK = 20
# FVG – uznajemy cenę za "bliską" luce gdy jest w strefie lub do X% powyżej dolnej granicy
FVG_NEAR_PCT = 0.03   # 3 %

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
        print(f"  S&P 500 (iShares IVV): {len(tickers)} spolok")
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
        print(f"  S&P 600 (iShares IJR): {len(tickers)} spolok")
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
        print(f"  Russell 2000: {len(tickers)} spółek")
        return tickers
    except Exception as e:
        print(f"  Russell 2000 błąd: {e}")
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
    smi = [
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
    all_tickers = list(set(dax + cac + ftse + aex + ibex + smi + mib + omx + obx + bel + wig))
    print(f"  EU statyczna lista: {len(all_tickers)} tickerow")
    print(f"    DAX:{len(dax)} CAC:{len(cac)} FTSE:{len(ftse)} AEX:{len(aex)} IBEX:{len(ibex)}")
    print(f"    SMI:{len(smi)} MIB:{len(mib)} OMX:{len(omx)} OBX:{len(obx)} BEL:{len(bel)} WIG:{len(wig)}")
    return all_tickers


# ══════════════════════════════════════════════════════════════
#  SMI INDICATOR  (port z Pine Script "SMI Signal Strategy")
#  Parametry domyślne: lengthK=10, lengthD=3, lengthEMA=3
# ══════════════════════════════════════════════════════════════

def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()

def ema_ema(series: pd.Series, length: int) -> pd.Series:
    """Podwójne EMA – odpowiednik emaEma() w Pine Script."""
    return ema(ema(series, length), length)

def calc_smi(high: pd.Series, low: pd.Series, close: pd.Series,
             length_k: int = 10, length_d: int = 3, length_ema: int = 3):
    """
    SMI = 200 * emaEma(relativeRange, lengthD) / emaEma(highestLowestRange, lengthD)
    smiEMA = EMA(SMI, lengthEMA)
    """
    highest_high        = high.rolling(length_k).max()
    lowest_low          = low.rolling(length_k).min()
    highest_lowest_range = highest_high - lowest_low
    relative_range      = close - (highest_high + lowest_low) / 2

    denom = ema_ema(highest_lowest_range, length_d)
    denom = denom.replace(0, np.nan)

    smi     = 200 * (ema_ema(relative_range, length_d) / denom)
    smi_ema = ema(smi, length_ema)
    return smi, smi_ema


def smi_signals(smi: pd.Series, smi_ema: pd.Series,
                use_cross: bool = True,
                use_zone:  bool = True,
                use_zero:  bool = False,
                strong_only: bool = False):
    """
    Zwraca (buy_signal, strong_buy, smi_val, smi_ema_val, zone_str)
    dla OSTATNIEJ świecy.
    """
    if len(smi) < 3:
        return False, False, None, None, "—"

    s0  = float(smi.iloc[-1])
    s1  = float(smi.iloc[-2])
    e0  = float(smi_ema.iloc[-1])
    e1  = float(smi_ema.iloc[-2])

    cross_up = (s1 < e1) and (s0 >= e0)
    exit_os  = (s1 < -40) and (s0 >= -40)
    zero_up  = (s1 < 0)   and (s0 >= 0)

    raw_buy = (
        (use_cross and cross_up) or
        (use_zone  and exit_os)  or
        (use_zero  and zero_up)
    )

    is_os       = s0 <= -40
    strong_buy  = raw_buy and (s1 <= -40 or is_os)
    buy_signal  = strong_only and strong_buy or (not strong_only and raw_buy)

    if s0 >= 40:
        zone = "OVERBOUGHT"
    elif s0 <= -40:
        zone = "OVERSOLD"
    elif s0 > 0:
        zone = "Bullish"
    else:
        zone = "Bearish"

    return buy_signal, strong_buy, round(s0, 2), round(e0, 2), zone


# ══════════════════════════════════════════════════════════════
#  FVG – FAIR VALUE GAP (tygodniowy interwał)
#
#  Definicja:
#    Bullish FVG: high[i-2] < low[i]   → luka wzrostowa
#                 strefa = [high[i-2], low[i]]
#    Bearish FVG: low[i-2]  > high[i]  → luka spadkowa
#                 strefa = [high[i], low[i-2]]
#
#  Luka jest "niezapełniona" dopóki żadna kolejna świeca nie
#  przetradowała całego jej obszaru (cena nie weszła w strefę
#  od przeciwnej strony i jej nie zamknęła).
#
#  Zwracamy OSTATNIĄ niezapełnioną bullish FVG w oknie lookback.
#  Sprawdzamy też, czy bieżąca cena jest wewnątrz strefy lub
#  tuż powyżej jej dolnej granicy (potencjalny test wsparcia).
# ══════════════════════════════════════════════════════════════

def calc_fvg(high: pd.Series, low: pd.Series, close: pd.Series,
             lookback: int = FVG_LOOKBACK,
             near_pct: float = FVG_NEAR_PCT):
    """
    Wykrywa niezapełnione Fair Value Gaps na danych tygodniowych.

    Zwraca słownik:
        fvg_bull        – czy istnieje niezapełniona bullish FVG (bool)
        fvg_bear        – czy istnieje niezapełniona bearish FVG (bool)
        fvg_bull_top    – górna granica ostatniej bullish FVG
        fvg_bull_bot    – dolna granica ostatniej bullish FVG
        fvg_bear_top    – górna granica ostatniej bearish FVG
        fvg_bear_bot    – dolna granica ostatniej bearish FVG
        fvg_bull_age    – ile tygodni temu powstała bullish FVG
        fvg_bear_age    – ile tygodni temu powstała bearish FVG
        fvg_price_in_bull – cena wewnątrz bullish FVG (bool)
        fvg_price_near_bull – cena tuż nad dolną granicą bullish FVG (bool)
        fvg_price_in_bear – cena wewnątrz bearish FVG (bool)
        fvg_confluence  – sygnał SMI + cena przy bullish FVG (bool, wypełniany zewnętrznie)
    """
    result = {
        "fvg_bull": False,
        "fvg_bear": False,
        "fvg_bull_top": None,
        "fvg_bull_bot": None,
        "fvg_bear_top": None,
        "fvg_bear_bot": None,
        "fvg_bull_age": None,
        "fvg_bear_age": None,
        "fvg_price_in_bull": False,
        "fvg_price_near_bull": False,
        "fvg_price_in_bear": False,
    }

    n = len(high)
    if n < 3:
        return result

    current_price = float(close.iloc[-1])

    # Pracujemy na ostatnich (lookback + 3) świecach
    window = min(n, lookback + 3)
    h = high.iloc[-window:].reset_index(drop=True)
    l = low.iloc[-window:].reset_index(drop=True)
    w = len(h)

    # ── Szukamy FVG iterując od świecy i=2 do w-1
    #    Świeca i jest ŚRODKOWĄ (napędową). Luka między i-2 a i.
    #    Po znalezieniu sprawdzamy, czy późniejsze świece ją zapełniły.

    last_bull = None   # (bot, top, age)
    last_bear = None

    for i in range(2, w):
        bot_b = float(h.iloc[i - 2])   # Bullish FVG: high[i-2]
        top_b = float(l.iloc[i])        #              low[i]
        if bot_b < top_b:
            # Potencjalna bullish FVG – sprawdzamy czy niezapełniona
            # przez świece i+1 … w-1
            filled = False
            for j in range(i + 1, w):
                # Wypełnienie = low[j] <= bot_b (cena weszła poniżej dolnej granicy FVG)
                if float(l.iloc[j]) <= bot_b:
                    filled = True
                    break
            if not filled:
                age = (w - 1) - i   # ile świec temu (0 = najnowsza)
                if last_bull is None or age < last_bull[2]:
                    last_bull = (bot_b, top_b, age)

        top_br = float(l.iloc[i - 2])  # Bearish FVG: low[i-2]
        bot_br = float(h.iloc[i])       #              high[i]
        if top_br > bot_br:
            filled = False
            for j in range(i + 1, w):
                # Wypełnienie = high[j] >= top_br
                if float(h.iloc[j]) >= top_br:
                    filled = True
                    break
            if not filled:
                age = (w - 1) - i
                if last_bear is None or age < last_bear[2]:
                    last_bear = (bot_br, top_br, age)

    # ── Bullish FVG
    if last_bull:
        bot, top, age = last_bull
        result["fvg_bull"]     = True
        result["fvg_bull_bot"] = round(bot, 4)
        result["fvg_bull_top"] = round(top, 4)
        result["fvg_bull_age"] = age
        # Cena wewnątrz strefy
        result["fvg_price_in_bull"]   = bot <= current_price <= top
        # Cena do near_pct% powyżej dolnej granicy (strefa wsparcia)
        result["fvg_price_near_bull"] = current_price <= bot * (1 + near_pct) and current_price >= bot * (1 - near_pct)

    # ── Bearish FVG
    if last_bear:
        bot, top, age = last_bear
        result["fvg_bear"]     = True
        result["fvg_bear_bot"] = round(bot, 4)
        result["fvg_bear_top"] = round(top, 4)
        result["fvg_bear_age"] = age
        result["fvg_price_in_bear"] = bot <= current_price <= top

    return result


# ══════════════════════════════════════════════════════════════
#  ANALIZA FUNDAMENTALNA
# ══════════════════════════════════════════════════════════════

def check_fundamentals(tkr_obj, min_annual_rev_growth: float = 0.15):
    try:
        q = tkr_obj.quarterly_financials
        if q is None or q.empty:
            return False, False, None, None, None, False

        rev_up = earn_up = False
        rev_vals = earn_vals = None
        annual_rev_growth = None

        if "Total Revenue" in q.index:
            rev = q.loc["Total Revenue"].dropna()
            if len(rev) >= 2:
                rev_up   = float(rev.iloc[0]) > float(rev.iloc[1])
                rev_vals = (round(float(rev.iloc[1]) / 1e6, 1),
                            round(float(rev.iloc[0]) / 1e6, 1))
            if len(rev) >= 8:
                ttm_curr = float(rev.iloc[0:4].sum())
                ttm_prev = float(rev.iloc[4:8].sum())
                if ttm_prev > 0:
                    annual_rev_growth = (ttm_curr - ttm_prev) / ttm_prev
            elif len(rev) >= 5:
                curr = float(rev.iloc[0])
                prev_year = float(rev.iloc[4])
                if prev_year > 0:
                    annual_rev_growth = (curr - prev_year) / prev_year

        if "Net Income" in q.index:
            net = q.loc["Net Income"].dropna()
            if len(net) >= 2:
                earn_up   = float(net.iloc[0]) > float(net.iloc[1])
                earn_vals = (round(float(net.iloc[1]) / 1e6, 1),
                             round(float(net.iloc[0]) / 1e6, 1))

        rev_annual_ok = (annual_rev_growth is not None
                         and annual_rev_growth >= min_annual_rev_growth)

        return rev_up, earn_up, rev_vals, earn_vals, annual_rev_growth, rev_annual_ok
    except:
        return False, False, None, None, None, False


# ══════════════════════════════════════════════════════════════
#  GŁÓWNA PĘTLA SCREENER
# ══════════════════════════════════════════════════════════════

def run_screener():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    start = datetime.now()
    print("=" * 60)
    print(f"SCREENER START: {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print("Wskaźnik: SMI Signal Strategy (lengthK=10, lengthD=3, lengthEMA=3)")
    print("Sygnały:  cross_up + exit_OS | strong = z głębi strefy OS")
    print("FVG:      Fair Value Gap (tygodniowy, lookback=20 świec)")
    print("=" * 60)

    print("\n[1/5] Pobieranie list spółek...")
    usa_tickers = list(set(get_sp500() + get_sp600() + get_russell2000()))
    eu_tickers  = list(set(get_european_indices()))
    all_tickers = [(t, "USA") for t in usa_tickers] + [(t, "EU") for t in eu_tickers]
    print(f"\nŁącznie: {len(all_tickers)} spółek ({len(usa_tickers)} USA, {len(eu_tickers)} EU)")

    print("\n[2/5] Analiza spółek...")
    results  = []
    signals  = []
    strong   = []
    skipped  = 0
    errors   = 0

    for i, (symbol, market) in enumerate(all_tickers):
        try:
            tkr   = yf.Ticker(symbol)
            fi    = tkr.fast_info
            price = getattr(fi, "last_price", None)

            if not price or price <= 0 or price > MAX_PRICE:
                skipped += 1
                continue

            currency = getattr(fi, "currency", "USD")

            rev_up, earn_up, rev_vals, earn_vals, annual_growth, rev_annual_ok = check_fundamentals(tkr)
            if not (rev_up and earn_up and rev_annual_ok):
                skipped += 1
                continue

            hist = tkr.history(period="2y", interval="1wk").dropna(subset=["High", "Low", "Close"])
            if len(hist) < 20:
                skipped += 1
                continue

            # ── SMI
            smi, smi_ema_ser = calc_smi(hist["High"], hist["Low"], hist["Close"])
            buy_sig, strong_sig, smi_val, smi_ema_val, zone = smi_signals(smi, smi_ema_ser)

            if smi_val is None:
                skipped += 1
                continue

            # ── FVG
            fvg = calc_fvg(hist["High"], hist["Low"], hist["Close"])
            # Confluence = sygnał SMI + cena przy / wewnątrz bullish FVG
            fvg["fvg_confluence"] = buy_sig and (
                fvg["fvg_price_in_bull"] or fvg["fvg_price_near_bull"]
            )

            try:
                info    = tkr.info
                name    = info.get("shortName", symbol)
                sector  = info.get("sector", "—")
                country = info.get("country", "—")
            except:
                name    = symbol
                sector  = "—"
                country = "—"

            annual_growth_pct = round(annual_growth * 100, 1) if annual_growth else None
            smi_above_ema     = smi_val > smi_ema_val if smi_ema_val else False

            row = {
                "ticker":               symbol,
                "name":                 name,
                "market":               market,
                "country":              country,
                "sector":               sector,
                "price":                round(price, 2),
                "currency":             currency,
                "smi":                  smi_val,
                "smi_ema":              smi_ema_val,
                "smi_above_ema":        smi_above_ema,
                "zone":                 zone,
                "signal":               buy_sig,
                "strong_signal":        strong_sig,
                "rev_prev":             rev_vals[0] if rev_vals else None,
                "rev_curr":             rev_vals[1] if rev_vals else None,
                "earn_prev":            earn_vals[0] if earn_vals else None,
                "earn_curr":            earn_vals[1] if earn_vals else None,
                "rev_yoy_pct":          annual_growth_pct,
                # FVG fields
                "fvg_bull":             fvg["fvg_bull"],
                "fvg_bull_bot":         fvg["fvg_bull_bot"],
                "fvg_bull_top":         fvg["fvg_bull_top"],
                "fvg_bull_age":         fvg["fvg_bull_age"],
                "fvg_price_in_bull":    fvg["fvg_price_in_bull"],
                "fvg_price_near_bull":  fvg["fvg_price_near_bull"],
                "fvg_bear":             fvg["fvg_bear"],
                "fvg_bear_bot":         fvg["fvg_bear_bot"],
                "fvg_bear_top":         fvg["fvg_bear_top"],
                "fvg_bear_age":         fvg["fvg_bear_age"],
                "fvg_price_in_bear":    fvg["fvg_price_in_bear"],
                "fvg_confluence":       fvg["fvg_confluence"],
                "scanned_at":           datetime.now().isoformat(),
            }
            results.append(row)

            growth_str  = f"+{annual_growth_pct}% YoY" if annual_growth_pct else ""
            sig_tag     = "STRONG" if strong_sig else ("SYGNAŁ" if buy_sig else "ok    ")
            fvg_tag     = " [FVG✓]" if fvg["fvg_confluence"] else (" [fvg]" if fvg["fvg_bull"] else "")
            print(f"  {sig_tag} [{i+1}] {symbol:10s} {market} | {price:6.2f} {currency} "
                  f"| SMI={smi_val:6.1f} EMA={smi_ema_val:6.1f} | {zone:12s} | {growth_str}{fvg_tag}")

            if buy_sig:
                signals.append(row)
            if strong_sig:
                strong.append(row)

            time.sleep(DELAY)

        except KeyboardInterrupt:
            print("\nPrzerwano przez użytkownika.")
            break
        except Exception as e:
            errors += 1

    elapsed = round((datetime.now() - start).total_seconds() / 60, 1)

    # Statystyki FVG
    fvg_bull_count   = sum(1 for r in results if r["fvg_bull"])
    fvg_conf_count   = sum(1 for r in results if r["fvg_confluence"])

    print(f"\n[3/5] Skanowanie zakończone w {elapsed} min")
    print(f"  Kandydaci: {len(results)} | Sygnały: {len(signals)} | Strong: {len(strong)} | "
          f"Pominięto: {skipped} | Błędy: {errors}")
    print(f"  FVG Bullish: {fvg_bull_count} | FVG+Sygnał (confluence): {fvg_conf_count}")

    print("\n[4/5] Zapis wyników...")
    meta = {
        "generated_at":   datetime.now().isoformat(),
        "elapsed_min":    elapsed,
        "total_scanned":  len(all_tickers),
        "candidates":     len(results),
        "signals":        len(signals),
        "strong":         len(strong),
        "skipped":        skipped,
        "errors":         errors,
        "indicator":      "SMI(10,3,3)",
        "fvg_bull_count": fvg_bull_count,
        "fvg_conf_count": fvg_conf_count,
    }
    for fname, data in [("meta", meta), ("results", results), ("signals", signals), ("strong", strong)]:
        with open(f"{OUTPUT_DIR}/{fname}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    if results:
        pd.DataFrame(results).to_csv(f"{OUTPUT_DIR}/results.csv", index=False)
    if signals:
        pd.DataFrame(signals).to_csv(f"{OUTPUT_DIR}/signals.csv", index=False)

    print("[5/5] Generowanie raportu HTML...")
    generate_html(meta, results, signals, strong)
    print(f"\nGotowe! Wyniki w katalogu: {OUTPUT_DIR}/")
    return signals


# ══════════════════════════════════════════════════════════════
#  RAPORT HTML
# ══════════════════════════════════════════════════════════════

def generate_html(meta, results, signals, strong):
    dt = datetime.fromisoformat(meta["generated_at"]).strftime("%d.%m.%Y %H:%M")

    def zone_badge(zone):
        cls = {"OVERBOUGHT": "zone-ob", "OVERSOLD": "zone-os",
               "Bullish": "zone-bull", "Bearish": "zone-bear"}.get(zone, "")
        return f'<span class="zone-badge {cls}">{zone}</span>'

    def fvg_badge_html(r):
        """Zwraca badge FVG dla tabeli."""
        if r.get("fvg_confluence"):
            return '<span class="badge-fvg-conf" title="SMI + cena przy Bullish FVG">FVG✦</span>'
        if r.get("fvg_price_in_bull"):
            return '<span class="badge-fvg-in" title="Cena wewnątrz Bullish FVG">FVG▲</span>'
        if r.get("fvg_price_near_bull"):
            return '<span class="badge-fvg-near" title="Cena blisko dolnej granicy Bullish FVG">FVG~</span>'
        if r.get("fvg_bull"):
            return '<span class="badge-fvg" title="Niezapełniona Bullish FVG w oknie 20 tygodni">FVG</span>'
        if r.get("fvg_price_in_bear"):
            return '<span class="badge-fvg-bear" title="Cena wewnątrz Bearish FVG (opór)">FVG▼</span>'
        return ''

    def fvg_summary_html(r):
        """Blok FVG do karty sygnału."""
        lines = []
        if r.get("fvg_bull"):
            bot  = r["fvg_bull_bot"]
            top  = r["fvg_bull_top"]
            age  = r["fvg_bull_age"]
            age_str = f"{age}W temu" if age else "bieżący"
            in_str = ""
            if r.get("fvg_price_in_bull"):
                in_str = ' <span style="color:var(--green);font-size:10px">● W STREFIE</span>'
            elif r.get("fvg_price_near_bull"):
                in_str = ' <span style="color:var(--amber);font-size:10px">~ BLISKO</span>'
            lines.append(
                f'<div class="fvg-row fvg-bull">'
                f'<span>▲ Bull FVG</span>'
                f'<span>{bot} – {top} ({age_str}){in_str}</span>'
                f'</div>'
            )
        if r.get("fvg_bear"):
            bot  = r["fvg_bear_bot"]
            top  = r["fvg_bear_top"]
            age  = r["fvg_bear_age"]
            age_str = f"{age}W temu" if age else "bieżący"
            in_str = ' <span style="color:var(--red);font-size:10px">● W STREFIE</span>' if r.get("fvg_price_in_bear") else ""
            lines.append(
                f'<div class="fvg-row fvg-bear">'
                f'<span>▼ Bear FVG</span>'
                f'<span>{bot} – {top} ({age_str}){in_str}</span>'
                f'</div>'
            )
        if not lines:
            return ""
        inner = "".join(lines)
        return f'<div class="fvg-block">{inner}</div>'

    def rows_html(data):
        if not data:
            return "<tr><td colspan='11' style='text-align:center;color:#888;padding:2rem'>Brak wyników</td></tr>"
        html = ""
        for r in data:
            strong_badge = '<span class="badge-strong">STRONG</span>' if r.get("strong_signal") else (
                           '<span class="badge-signal">BUY</span>'   if r.get("signal")        else "")
            rev_str  = f"{r['rev_prev']} → {r['rev_curr']} M"  if r.get("rev_curr")  else "—"
            earn_str = f"{r['earn_prev']} → {r['earn_curr']} M" if r.get("earn_curr") else "—"
            smi_cls  = "smi-above" if r["smi_above_ema"] else "smi-below"
            fvg_b    = fvg_badge_html(r)
            html += f"""
            <tr>
              <td><span class="ticker">{r['ticker']}</span>{strong_badge}</td>
              <td class="name-col">{r['name']}</td>
              <td><span class="badge-{'usa' if r['market']=='USA' else 'eu'}">{r['market']}</span></td>
              <td>{r['sector']}</td>
              <td class="num">{r['price']} {r['currency']}</td>
              <td class="num {smi_cls}">{r['smi']}</td>
              <td class="num">{r['smi_ema']}</td>
              <td>{zone_badge(r['zone'])}</td>
              <td class="num">{fvg_b}</td>
              <td class="num">{rev_str}</td>
              <td class="num">{earn_str}</td>
            </tr>"""
        return html

    def signal_cards(data):
        if not data:
            return "<div class='empty'>Brak sygnałów w tym skanie</div>"
        cards = ""
        for r in sorted(data, key=lambda x: (-x.get("fvg_confluence", 0),
                                               -x.get("strong_signal", 0))):
            market_cls   = "usa" if r["market"] == "USA" else "eu"
            rev_str      = f"{r['rev_prev']} &rarr; {r['rev_curr']} M" if r.get("rev_curr")  else "&mdash;"
            earn_str     = f"{r['earn_prev']} &rarr; {r['earn_curr']} M" if r.get("earn_curr") else "&mdash;"

            # Kolor paska górnego: confluence > strong > buy
            if r.get("fvg_confluence"):
                top_color = "linear-gradient(90deg,#9b59b6,#00e599)"
            elif r.get("strong_signal"):
                top_color = "linear-gradient(90deg,#ff6b00,#ffb800)"
            else:
                top_color = "linear-gradient(90deg,#00c8ff,#00e599)"

            sig_label = "STRONG BUY" if r.get("strong_signal") else "BUY SIGNAL"
            sig_color = "#ffb800"    if r.get("strong_signal") else "#00c8ff"
            if r.get("fvg_confluence"):
                sig_label = ("STRONG + FVG✦" if r.get("strong_signal") else "BUY + FVG✦")
                sig_color = "#b07fff"

            yoy_str  = f"+{r['rev_yoy_pct']}% YoY" if r.get("rev_yoy_pct") else "—"
            zone     = r.get("zone", "—")
            zone_color = {"OVERBOUGHT": "#ff4560", "OVERSOLD": "#00e599",
                          "Bullish": "#4da6ff", "Bearish": "#ffa040"}.get(zone, "#888")

            fvg_section = fvg_summary_html(r)

            cards += (
                f'<div class="signal-card">'
                f'<div style="position:absolute;top:0;left:0;right:0;height:2px;background:{top_color}"></div>'
                f'<div style="display:flex;justify-content:space-between;align-items:flex-start">'
                f'<div><div class="sc-ticker">{r["ticker"]}</div>'
                f'<div class="sc-name">{r["name"]}</div></div>'
                f'<span class="badge-{market_cls}">{r["market"]}</span></div>'
                f'<div class="sc-price">{r["price"]} {r["currency"]}</div>'
                f'<div class="sc-row"><span>Sygnał</span><span style="color:{sig_color};font-weight:500">{sig_label}</span></div>'
                f'<div class="sc-row"><span>Strefa</span><span style="color:{zone_color}">{zone}</span></div>'
                f'<div class="sc-row"><span>Sektor</span><span>{r["sector"]}</span></div>'
                f'<div class="sc-row"><span>Revenue QoQ</span><span style="color:var(--green)">{rev_str}</span></div>'
                f'<div class="sc-row"><span>Net Income QoQ</span><span style="color:var(--green)">{earn_str}</span></div>'
                f'<div class="sc-row"><span>Rev. wzrost YoY</span><span style="color:var(--green)">{yoy_str}</span></div>'
                f'<div class="sc-stoch">'
                f'<div class="sc-stoch-item"><div class="sc-stoch-label">SMI</div>'
                f'<div class="sc-stoch-val {"green" if r["smi_above_ema"] else "red"}">{r["smi"]}</div></div>'
                f'<div class="sc-stoch-item"><div class="sc-stoch-label">SMI EMA</div>'
                f'<div class="sc-stoch-val">{r["smi_ema"]}</div></div>'
                f'</div>'
                f'{fvg_section}'
                f'</div>'
            )
        return f'<div class="signal-grid">{cards}</div>'

    # Sortowanie: confluence na górze, potem strong, potem zwykłe
    all_rows    = rows_html(sorted(results, key=lambda x: (
        -x.get("fvg_confluence", 0), -x.get("strong_signal", 0), x["smi"])))
    signal_rows = rows_html(sorted(signals, key=lambda x: (
        -x.get("fvg_confluence", 0), -x.get("strong_signal", 0))))
    cards_html  = signal_cards(signals)

    sc   = meta["signals"]
    str_ = meta["strong"]
    cc   = meta["candidates"]
    tc   = meta["total_scanned"]
    el   = meta["elapsed_min"]
    fbc  = meta.get("fvg_bull_count", 0)
    fcc  = meta.get("fvg_conf_count", 0)

    html = f"""<!DOCTYPE html>
<html lang="pl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Stock Screener SMI + FVG – {dt}</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@300;400;500&display=swap" rel="stylesheet">
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  :root {{
    --bg: #0a0e14; --bg2: #111620; --bg3: #1a2030;
    --border: #1e2d45; --text: #c8d8f0; --muted: #4a6080;
    --accent: #00c8ff; --green: #00e599; --amber: #ffb800;
    --red: #ff4560; --usa: #3b82f6; --eu: #10b981;
    --purple: #9b59b6; --fvg: #b07fff;
  }}
  body {{ background:var(--bg); color:var(--text); font-family:'IBM Plex Sans',sans-serif; font-size:14px; line-height:1.6; }}

  .header {{ border-bottom:1px solid var(--border); padding:2rem 2.5rem 1.5rem; display:flex; align-items:flex-start; justify-content:space-between; flex-wrap:wrap; gap:1rem; }}
  .header-left h1 {{ font-family:'IBM Plex Mono',monospace; font-size:22px; font-weight:500; color:#fff; letter-spacing:-.5px; }}
  .header-left h1 span {{ color:var(--accent); }}
  .header-left p {{ font-size:12px; color:var(--muted); margin-top:4px; font-family:'IBM Plex Mono',monospace; }}
  .criteria-pills {{ display:flex; flex-wrap:wrap; gap:6px; margin-top:12px; }}
  .pill {{ font-family:'IBM Plex Mono',monospace; font-size:11px; padding:3px 10px; border:1px solid var(--border); border-radius:100px; color:var(--muted); }}
  .pill.active {{ border-color:var(--accent); color:var(--accent); }}
  .pill.fvg {{ border-color:var(--fvg); color:var(--fvg); }}

  .stats {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(130px,1fr)); gap:1px; background:var(--border); border-bottom:1px solid var(--border); }}
  .stat {{ background:var(--bg); padding:1.25rem 1.5rem; }}
  .stat-label {{ font-size:11px; font-family:'IBM Plex Mono',monospace; color:var(--muted); text-transform:uppercase; letter-spacing:1px; }}
  .stat-value {{ font-size:28px; font-weight:300; color:#fff; margin-top:4px; font-family:'IBM Plex Mono',monospace; }}
  .stat-value.highlight {{ color:var(--accent); }}
  .stat-value.green {{ color:var(--green); }}
  .stat-value.amber {{ color:var(--amber); }}
  .stat-value.purple {{ color:var(--fvg); }}
  .stat-sub {{ font-size:11px; color:var(--muted); margin-top:2px; }}

  .tabs {{ display:flex; border-bottom:1px solid var(--border); padding:0 2rem; }}
  .tab {{ font-family:'IBM Plex Mono',monospace; font-size:12px; padding:12px 20px; cursor:pointer; color:var(--muted); border-bottom:2px solid transparent; margin-bottom:-1px; background:none; border-top:none; border-left:none; border-right:none; transition:all .15s; }}
  .tab:hover {{ color:var(--text); }}
  .tab.active {{ color:var(--accent); border-bottom-color:var(--accent); }}

  .content {{ padding:1.5rem 2rem; }}
  .panel {{ display:none; }}
  .panel.active {{ display:block; }}

  .toolbar {{ display:flex; gap:10px; margin-bottom:1rem; flex-wrap:wrap; align-items:center; }}
  .search-input {{ background:var(--bg2); border:1px solid var(--border); border-radius:6px; padding:7px 12px; color:var(--text); font-family:'IBM Plex Mono',monospace; font-size:12px; width:240px; outline:none; }}
  .search-input:focus {{ border-color:var(--accent); }}
  .filter-select {{ background:var(--bg2); border:1px solid var(--border); border-radius:6px; padding:7px 12px; color:var(--text); font-family:'IBM Plex Mono',monospace; font-size:12px; outline:none; cursor:pointer; }}
  .count-label {{ font-family:'IBM Plex Mono',monospace; font-size:11px; color:var(--muted); margin-left:auto; }}

  .table-wrap {{ overflow-x:auto; }}
  table {{ width:100%; border-collapse:collapse; font-size:13px; }}
  thead th {{ font-family:'IBM Plex Mono',monospace; font-size:10px; text-transform:uppercase; letter-spacing:1px; color:var(--muted); text-align:left; padding:10px 12px; border-bottom:1px solid var(--border); white-space:nowrap; cursor:pointer; user-select:none; }}
  thead th:hover {{ color:var(--text); }}
  tbody tr {{ border-bottom:1px solid var(--border); transition:background .1s; }}
  tbody tr:hover {{ background:var(--bg2); }}
  td {{ padding:10px 12px; vertical-align:middle; }}
  .ticker {{ font-family:'IBM Plex Mono',monospace; font-weight:500; color:#fff; font-size:13px; }}
  .name-col {{ max-width:180px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; color:var(--muted); font-size:12px; }}
  .num {{ font-family:'IBM Plex Mono',monospace; font-size:12px; text-align:right; }}
  .smi-above {{ color:var(--green); }}
  .smi-below {{ color:var(--red); }}

  .badge-usa {{ display:inline-block; font-size:10px; font-family:'IBM Plex Mono',monospace; padding:2px 7px; border-radius:4px; background:rgba(59,130,246,.15); color:var(--usa); border:1px solid rgba(59,130,246,.3); }}
  .badge-eu  {{ display:inline-block; font-size:10px; font-family:'IBM Plex Mono',monospace; padding:2px 7px; border-radius:4px; background:rgba(16,185,129,.15); color:var(--eu);  border:1px solid rgba(16,185,129,.3); }}
  .badge-signal {{ display:inline-block; font-size:9px; font-family:'IBM Plex Mono',monospace; padding:1px 6px; border-radius:4px; background:rgba(0,200,255,.15); color:var(--accent); border:1px solid rgba(0,200,255,.3); margin-left:6px; vertical-align:middle; animation:pulse 2s infinite; }}
  .badge-strong {{ display:inline-block; font-size:9px; font-family:'IBM Plex Mono',monospace; padding:1px 6px; border-radius:4px; background:rgba(255,184,0,.15); color:var(--amber); border:1px solid rgba(255,184,0,.4); margin-left:6px; vertical-align:middle; animation:pulse 1.5s infinite; }}

  /* FVG badges */
  .badge-fvg-conf {{ display:inline-block; font-size:9px; font-family:'IBM Plex Mono',monospace; padding:1px 6px; border-radius:4px; background:rgba(176,127,255,.2); color:var(--fvg); border:1px solid rgba(176,127,255,.5); white-space:nowrap; animation:pulse 1.5s infinite; }}
  .badge-fvg-in   {{ display:inline-block; font-size:9px; font-family:'IBM Plex Mono',monospace; padding:1px 6px; border-radius:4px; background:rgba(0,229,153,.15); color:var(--green); border:1px solid rgba(0,229,153,.4); white-space:nowrap; }}
  .badge-fvg-near {{ display:inline-block; font-size:9px; font-family:'IBM Plex Mono',monospace; padding:1px 6px; border-radius:4px; background:rgba(255,184,0,.12); color:var(--amber); border:1px solid rgba(255,184,0,.35); white-space:nowrap; }}
  .badge-fvg      {{ display:inline-block; font-size:9px; font-family:'IBM Plex Mono',monospace; padding:1px 6px; border-radius:4px; background:rgba(155,89,182,.12); color:#c07fff; border:1px solid rgba(155,89,182,.3); white-space:nowrap; }}
  .badge-fvg-bear {{ display:inline-block; font-size:9px; font-family:'IBM Plex Mono',monospace; padding:1px 6px; border-radius:4px; background:rgba(255,69,96,.12); color:var(--red); border:1px solid rgba(255,69,96,.3); white-space:nowrap; }}

  @keyframes pulse {{ 0%,100%{{opacity:1}} 50%{{opacity:.45}} }}

  .zone-badge {{ display:inline-block; font-size:10px; font-family:'IBM Plex Mono',monospace; padding:2px 8px; border-radius:4px; white-space:nowrap; }}
  .zone-ob   {{ background:rgba(255,69,96,.12);  color:#ff4560; border:1px solid rgba(255,69,96,.3); }}
  .zone-os   {{ background:rgba(0,229,153,.12);  color:#00e599; border:1px solid rgba(0,229,153,.3); }}
  .zone-bull {{ background:rgba(77,166,255,.12); color:#4da6ff; border:1px solid rgba(77,166,255,.3); }}
  .zone-bear {{ background:rgba(255,160,64,.12); color:#ffa040; border:1px solid rgba(255,160,64,.3); }}

  .signal-grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(260px,1fr)); gap:12px; margin-bottom:1.5rem; }}
  .signal-card {{ background:var(--bg2); border:1px solid var(--border); border-radius:8px; padding:1rem 1.25rem; position:relative; overflow:hidden; }}
  .sc-ticker {{ font-family:'IBM Plex Mono',monospace; font-size:18px; font-weight:500; color:#fff; }}
  .sc-name {{ font-size:12px; color:var(--muted); margin-top:2px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
  .sc-price {{ font-family:'IBM Plex Mono',monospace; font-size:20px; font-weight:300; color:var(--accent); margin:10px 0 8px; }}
  .sc-row {{ display:flex; justify-content:space-between; font-size:12px; color:var(--muted); margin-top:4px; }}
  .sc-row span:last-child {{ font-family:'IBM Plex Mono',monospace; color:var(--text); }}
  .sc-stoch {{ display:flex; gap:12px; margin-top:10px; padding-top:10px; border-top:1px solid var(--border); }}
  .sc-stoch-item {{ flex:1; }}
  .sc-stoch-label {{ font-size:10px; font-family:'IBM Plex Mono',monospace; color:var(--muted); text-transform:uppercase; letter-spacing:.5px; }}
  .sc-stoch-val {{ font-family:'IBM Plex Mono',monospace; font-size:16px; font-weight:500; margin-top:2px; }}
  .sc-stoch-val.green {{ color:var(--green); }}
  .sc-stoch-val.red {{ color:var(--red); }}

  /* FVG block w karcie */
  .fvg-block {{ margin-top:10px; padding-top:10px; border-top:1px solid var(--border); display:flex; flex-direction:column; gap:5px; }}
  .fvg-row {{ display:flex; justify-content:space-between; font-size:11px; font-family:'IBM Plex Mono',monospace; gap:8px; flex-wrap:wrap; }}
  .fvg-row span:first-child {{ font-weight:500; white-space:nowrap; }}
  .fvg-row span:last-child {{ color:var(--muted); text-align:right; }}
  .fvg-bull span:first-child {{ color:var(--fvg); }}
  .fvg-bear span:first-child {{ color:var(--red); }}

  /* Legenda FVG */
  .fvg-legend {{ display:flex; flex-wrap:wrap; gap:8px; margin-bottom:1rem; font-size:11px; font-family:'IBM Plex Mono',monospace; color:var(--muted); align-items:center; }}
  .fvg-legend span {{ color:var(--text); }}

  .empty {{ text-align:center; padding:4rem 2rem; color:var(--muted); font-family:'IBM Plex Mono',monospace; font-size:13px; }}
  .empty::before {{ content:'//'; display:block; font-size:32px; margin-bottom:1rem; color:var(--border); }}

  footer {{ border-top:1px solid var(--border); padding:1rem 2rem; font-size:11px; color:var(--muted); font-family:'IBM Plex Mono',monospace; display:flex; justify-content:space-between; flex-wrap:wrap; gap:8px; }}

  /* ── AI ANALIZA PANEL ───────────────────────────────────── */
  .ai-panel-wrap {{ max-width:900px; }}
  .ai-intro {{ font-size:13px; color:var(--muted); margin-bottom:1.5rem; line-height:1.7; }}
  .ai-intro strong {{ color:var(--text); }}
  .ai-key-row {{ display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin-bottom:1rem; }}
  .ai-key-input {{ background:var(--bg2); border:1px solid var(--border); border-radius:6px; padding:8px 14px; color:var(--text); font-family:'IBM Plex Mono',monospace; font-size:12px; width:340px; outline:none; letter-spacing:.05em; }}
  .ai-key-input:focus {{ border-color:var(--fvg); box-shadow:0 0 0 2px rgba(176,127,255,.15); }}
  .ai-run-btn {{ background:linear-gradient(135deg,rgba(176,127,255,.2),rgba(0,200,255,.15)); border:1px solid rgba(176,127,255,.4); border-radius:6px; padding:8px 20px; color:var(--fvg); font-family:'IBM Plex Mono',monospace; font-size:12px; cursor:pointer; white-space:nowrap; transition:all .2s; }}
  .ai-run-btn:hover:not(:disabled) {{ background:linear-gradient(135deg,rgba(176,127,255,.35),rgba(0,200,255,.25)); border-color:var(--fvg); }}
  .ai-run-btn:disabled {{ opacity:.5; cursor:not-allowed; }}
  .ai-key-note {{ font-size:11px; color:var(--muted); font-family:'IBM Plex Mono',monospace; }}
  .ai-status {{ display:none; font-size:12px; font-family:'IBM Plex Mono',monospace; padding:6px 12px; border-radius:5px; margin-bottom:.75rem; }}
  .ai-status.ok    {{ background:rgba(0,229,153,.12); color:var(--green); border:1px solid rgba(0,229,153,.25); }}
  .ai-status.error {{ background:rgba(255,69,96,.12);  color:var(--red);   border:1px solid rgba(255,69,96,.25); }}
  .ai-output {{ min-height:80px; }}
  .ai-loading {{ display:flex; flex-direction:column; align-items:center; padding:3rem; color:var(--muted); font-family:'IBM Plex Mono',monospace; font-size:12px; }}
  .ai-dots {{ display:flex; gap:6px; }}
  .ai-dots span {{ width:8px; height:8px; border-radius:50%; background:var(--fvg); animation:ai-bounce .9s ease-in-out infinite; }}
  .ai-dots span:nth-child(2) {{ animation-delay:.15s; }}
  .ai-dots span:nth-child(3) {{ animation-delay:.30s; }}
  @keyframes ai-bounce {{ 0%,100%{{transform:translateY(0);opacity:.4}} 50%{{transform:translateY(-6px);opacity:1}} }}
  .ai-spin {{ display:inline-block; animation:ai-rotate 1s linear infinite; }}
  @keyframes ai-rotate {{ to{{transform:rotate(360deg)}} }}
  .ai-cursor {{ animation:ai-blink 1s step-end infinite; color:var(--fvg); }}
  @keyframes ai-blink {{ 50%{{opacity:0}} }}
  .ai-error {{ background:rgba(255,69,96,.08); border:1px solid rgba(255,69,96,.25); border-radius:6px; padding:1rem 1.25rem; color:var(--red); font-size:13px; }}
  .ai-result {{ background:var(--bg2); border:1px solid var(--border); border-radius:8px; padding:1.75rem 2rem; line-height:1.75; }}
  .ai-h2 {{ font-family:'IBM Plex Mono',monospace; font-size:15px; font-weight:500; color:var(--fvg); margin:1.5rem 0 .5rem; padding-bottom:6px; border-bottom:1px solid var(--border); }}
  .ai-h2:first-child {{ margin-top:0; }}
  .ai-h3 {{ font-family:'IBM Plex Mono',monospace; font-size:13px; font-weight:500; color:var(--accent); margin:.75rem 0 .25rem; }}
  .ai-p {{ margin:.35rem 0; font-size:13.5px; color:var(--text); }}
  .ai-gap {{ height:.5rem; }}
  .ai-ul, .ai-ol {{ margin:.4rem 0 .4rem 1.25rem; padding:0; }}
  .ai-ul li, .ai-ol li {{ font-size:13.5px; color:var(--text); margin:.2rem 0; }}
  .ai-num {{ font-family:'IBM Plex Mono',monospace; color:var(--fvg); font-size:12px; margin-right:4px; }}
  .ai-code {{ font-family:'IBM Plex Mono',monospace; font-size:12px; background:rgba(255,255,255,.07); padding:1px 5px; border-radius:3px; color:var(--amber); }}
</style>
</head>
<body>

<div class="header">
  <div class="header-left">
    <h1>STOCK <span>SCREENER</span> // SMI + FVG</h1>
    <p>// generated {dt} &nbsp;|&nbsp; elapsed {el} min &nbsp;|&nbsp; SMI(10,3,3) weekly &nbsp;|&nbsp; FVG lookback 20W</p>
    <div class="criteria-pills">
      <span class="pill active">USA + Europa</span>
      <span class="pill active">cena ≤ 100 USD/EUR</span>
      <span class="pill active">Revenue ↑ QoQ</span>
      <span class="pill active">Net Income ↑ QoQ</span>
      <span class="pill active">Rev YoY ≥ 15%</span>
      <span class="pill active">SMI cross / exit OS (1W)</span>
      <span class="pill fvg">FVG Bullish (niezapełniony)</span>
      <span class="pill fvg">FVG✦ = SMI + cena przy luce</span>
    </div>
  </div>
</div>

<div class="stats">
  <div class="stat"><div class="stat-label">Przeskanowano</div><div class="stat-value">{tc}</div><div class="stat-sub">USA + EU</div></div>
  <div class="stat"><div class="stat-label">Kandydaci</div><div class="stat-value highlight">{cc}</div><div class="stat-sub">fundamenty OK</div></div>
  <div class="stat"><div class="stat-label">Sygnały BUY</div><div class="stat-value green">{sc}</div><div class="stat-sub">cross / exit OS</div></div>
  <div class="stat"><div class="stat-label">Strong BUY</div><div class="stat-value amber">{str_}</div><div class="stat-sub">z głębi strefy OS</div></div>
  <div class="stat"><div class="stat-label">Bull FVG</div><div class="stat-value purple">{fbc}</div><div class="stat-sub">niezapełnione luki</div></div>
  <div class="stat"><div class="stat-label">FVG✦ Konfluencja</div><div class="stat-value purple">{fcc}</div><div class="stat-sub">SMI + cena przy FVG</div></div>
  <div class="stat"><div class="stat-label">Czas skanu</div><div class="stat-value">{el}</div><div class="stat-sub">minut</div></div>
</div>

<div class="tabs">
  <button class="tab active" onclick="switchTab('signals',this)">Sygnały BUY ({sc})</button>
  <button class="tab" onclick="switchTab('all',this)">Wszyscy kandydaci ({cc})</button>
  <button class="tab" onclick="switchTab('ai',this)" style="color:var(--fvg);margin-left:auto">✦ AI Analiza</button>
</div>

<div class="content">

  <div id="panel-signals" class="panel active">
    <div class="fvg-legend">
      FVG legenda:
      <span class="badge-fvg-conf">FVG✦</span> <span>Konfluencja – SMI + cena przy Bullish FVG (najsilniejszy sygnał)</span>
      <span class="badge-fvg-in">FVG▲</span> <span>Cena wewnątrz Bullish FVG (wsparcie)</span>
      <span class="badge-fvg-near">FVG~</span> <span>Cena blisko dolnej granicy FVG (±3%)</span>
      <span class="badge-fvg">FVG</span> <span>Niezapełniona Bullish FVG w oknie 20W</span>
      <span class="badge-fvg-bear">FVG▼</span> <span>Cena w Bearish FVG (opór)</span>
    </div>
    {cards_html}
    <div class="table-wrap">
      <table><thead><tr>
        <th>Ticker</th><th>Nazwa</th><th>Rynek</th><th>Sektor</th>
        <th style="text-align:right">Cena</th>
        <th style="text-align:right">SMI</th>
        <th style="text-align:right">EMA</th>
        <th>Strefa</th>
        <th>FVG</th>
        <th style="text-align:right">Revenue (M)</th>
        <th style="text-align:right">Net Inc. (M)</th>
      </tr></thead><tbody>{signal_rows}</tbody></table>
    </div>
  </div>

  <div id="panel-all" class="panel">
    <div class="toolbar">
      <input class="search-input" type="text" placeholder="szukaj tickera lub nazwy..." oninput="filterTable(this.value)"/>
      <select class="filter-select" onchange="filterMarket(this.value)">
        <option value="">Wszystkie rynki</option>
        <option value="USA">USA</option>
        <option value="EU">EU</option>
      </select>
      <select class="filter-select" onchange="filterZone(this.value)">
        <option value="">Wszystkie strefy</option>
        <option value="OVERSOLD">OVERSOLD</option>
        <option value="Bearish">Bearish</option>
        <option value="Bullish">Bullish</option>
        <option value="OVERBOUGHT">OVERBOUGHT</option>
      </select>
      <select class="filter-select" onchange="filterFvg(this.value)">
        <option value="">Wszystkie FVG</option>
        <option value="FVG✦">FVG✦ Konfluencja</option>
        <option value="FVG▲">FVG▲ W strefie</option>
        <option value="FVG~">FVG~ Blisko</option>
        <option value="FVG">FVG Bullish</option>
      </select>
      <span class="count-label" id="row-count">{cc} wyników</span>
    </div>
    <div class="table-wrap">
      <table id="tbl-all">
        <thead><tr>
          <th>Ticker</th><th>Nazwa</th><th>Rynek</th><th>Sektor</th>
          <th style="text-align:right">Cena</th>
          <th style="text-align:right">SMI</th>
          <th style="text-align:right">EMA</th>
          <th>Strefa</th>
          <th>FVG</th>
          <th style="text-align:right">Revenue (M)</th>
          <th style="text-align:right">Net Inc. (M)</th>
        </tr></thead>
        <tbody id="tbody-all">{all_rows}</tbody>
      </table>
    </div>
  </div>

  <!-- ── PANEL AI ─────────────────────────────────────────── -->
  <div id="panel-ai" class="panel">
    <div class="ai-panel-wrap">
      <p class="ai-intro">
        Wklej swój klucz <strong>Anthropic API</strong> — Claude przeanalizuje wszystkie {sc} sygnałów
        z tego skanu: sektory, ryzyka, FVG confluence i ranking wejść.<br>
        <span style="color:var(--muted);font-size:11px;font-family:'IBM Plex Mono',monospace">
          // Klucz używany wyłącznie w tej sesji przeglądarki — nie jest zapisywany nigdzie.
        </span>
      </p>
      <div class="ai-key-row">
        <input type="password" id="ai-key-input" class="ai-key-input"
               placeholder="sk-ant-api03-..."
               onkeydown="if(event.key==='Enter') runAiAnalysis()"/>
        <button id="ai-run-btn" class="ai-run-btn" onclick="runAiAnalysis()">✦ Analizuj z Claude</button>
        <span class="ai-key-note">// claude-sonnet-4 | max 4000 tokenów</span>
      </div>
      <div id="ai-status" class="ai-status"></div>
      <div id="ai-output" class="ai-output"></div>
    </div>
  </div>

</div>

<footer>
  <span>// stock-screener &nbsp;|&nbsp; SMI(10,3,3) + FVG(20W) &nbsp;|&nbsp; dane: Yahoo Finance</span>
  <span>Nie stanowi porady inwestycyjnej</span>
</footer>

<script>
__TABS_JS__
__AI_JS__
</script>
</body>
</html>"""

    # Wstrzyknięcie kodu AI JS poza f-stringiem (brak podwajania nawiasów)
    signals_json = json.dumps(signals, ensure_ascii=False)
    meta_json    = json.dumps(meta,    ensure_ascii=False)
    html = html.replace("__TABS_JS__", build_tabs_js())
    html = html.replace("__AI_JS__",   build_ai_js(signals_json, meta_json))

    path = f"{OUTPUT_DIR}/index.html"
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  Raport: {path}")


# ══════════════════════════════════════════════════════════════
#  GENERATOR KODU JS DLA ANALIZY AI
#  Budowany poza f-stringiem HTML, żeby uniknąć konieczności
#  podwajania wszystkich nawiasów klamrowych ({{}}).
# ══════════════════════════════════════════════════════════════

def build_tabs_js() -> str:
    """Zwraca JS obsługujący zakładki i filtry tabeli."""
    return """
function switchTab(name, btn) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  btn.classList.add('active');
  document.getElementById('panel-' + name).classList.add('active');
}
let mf = '', zf = '', ff = '';
function filterMarket(v) { mf = v; applyFilters(); }
function filterZone(v)   { zf = v; applyFilters(); }
function filterFvg(v)    { ff = v; applyFilters(); }
function filterTable(v)  { applyFilters(v); }
function applyFilters(search) {
  const rows = document.querySelectorAll('#tbody-all tr');
  let visible = 0;
  const s = (search ?? document.querySelector('.search-input').value).toLowerCase();
  rows.forEach(row => {
    const t = row.textContent.toLowerCase();
    const ok = (!mf || t.includes(mf.toLowerCase()))
            && (!zf || t.includes(zf.toLowerCase()))
            && (!ff || row.innerHTML.includes(ff))
            && (!s  || t.includes(s));
    row.style.display = ok ? '' : 'none';
    if (ok) visible++;
  });
  document.getElementById('row-count').textContent = visible + ' wyników';
}
"""

def build_ai_js(signals_json: str, meta_json: str) -> str:
    """Zwraca blok JavaScript obsługujący analizę AI w przeglądarce."""
    return (
        "const SIGNALS_DATA = " + signals_json + ";\n"
        "const META_DATA = "    + meta_json    + ";\n\n"
        """
// ── Budowanie promptu ────────────────────────────────────────
function buildPrompt() {
  const sc = SIGNALS_DATA;
  if (!sc || sc.length === 0) return null;

  const sigLines = sc.map(s => {
    const fvgTag = s.fvg_confluence      ? '[FVG✦ KONFLUENCJA]'  :
                   s.fvg_price_in_bull   ? '[FVG▲ w strefie]'    :
                   s.fvg_price_near_bull ? '[FVG~ blisko]'        :
                   s.fvg_bull            ? '[FVG bullish]'        :
                   s.fvg_price_in_bear   ? '[FVG▼ opór]'         : '';
    const sigType  = s.strong_signal ? 'STRONG BUY' : 'BUY';
    const rev      = s.rev_curr  != null ? `Rev: ${s.rev_prev}→${s.rev_curr}M`   : '';
    const earn     = s.earn_curr != null ? `NetInc: ${s.earn_prev}→${s.earn_curr}M` : '';
    const yoy      = s.rev_yoy_pct != null ? `YoY: +${s.rev_yoy_pct}%` : '';
    const fvgZone  = s.fvg_bull
      ? `FVG-bull: ${s.fvg_bull_bot}–${s.fvg_bull_top} (${s.fvg_bull_age}W temu)` : '';
    return `- ${s.ticker} (${s.name}) | ${s.market} | ${s.sector} | ${s.price} ${s.currency}`
         + ` | SMI=${s.smi} EMA=${s.smi_ema} | Strefa: ${s.zone} | ${sigType} ${fvgTag}`
         + ` | ${rev} | ${earn} | ${yoy} | ${fvgZone}`.trimEnd();
  }).join('\\n');

  const m = META_DATA;
  const dt = m.generated_at ? m.generated_at.slice(0, 10) : '—';

  return `Jesteś doświadczonym analitykiem rynków akcji i traderem technicznym. Przeanalizuj wyniki automatycznego screener giełdowego i przygotuj pełną analizę inwestycyjną.

PARAMETRY SKANU:
- Data: ${dt}
- Przeskanowano: ${m.total_scanned} spółek (USA + Europa)
- Kandydaci (fundamenty OK): ${m.candidates}
- Sygnały BUY: ${m.signals} | Strong BUY: ${m.strong}
- Niezapełnione Bullish FVG: ${m.fvg_bull_count} | FVG✦ Konfluencja: ${m.fvg_conf_count}
- Wskaźnik: SMI(10,3,3) interwał tygodniowy
- Kryteria fundamentalne: Revenue ↑ QoQ + Net Income ↑ QoQ + Rev YoY ≥ 15%

LISTA SYGNAŁÓW (${sc.length} pozycji):
${sigLines}

Przygotuj analizę w języku polskim zgodnie z poniższą strukturą:

## 1. Ogólna ocena skanu
Oceń jakość i siłę obecnego skanu: stosunek sygnałów do kandydatów, dominujące strefy SMI, rozkład USA vs EU. Co to mówi o aktualnym sentymencie rynkowym?

## 2. Analiza sektorowa
Które sektory dominują wśród sygnałów? Czy to przypadkowe, czy sygnalizuje szerszy trend makroekonomiczny? Które sektory budzą Twoje szczególne zainteresowanie?

## 3. FVG Confluence — najsilniejsze setupy techniczne
Szczegółowo opisz każdą spółkę z oznaczeniem FVG✦ (SMI crossover + cena przy niezapełnionej Bullish FVG). Wyjaśnij dlaczego połączenie tych dwóch sygnałów jest technicznie wyjątkowe. Oceń siłę każdego setupu.

## 4. Ranking Top 5 sygnałów
Dla każdego z 5 najlepszych sygnałów podaj:
- **Ticker i uzasadnienie wyboru**
- **Mocne strony** (techniczne + fundamentalne)
- **Ryzyka i słabe punkty**
- **Sugerowany poziom wejścia** (przy FVG, przy cenie bieżącej, czy czekamy na cofnięcie?)

## 5. Sygnały ostrzegawcze
Które spółki spełniły kryteria screener, ale budzą wątpliwości? Podaj konkretne powody (np. mała dynamika wzrostu, strefa OVERBOUGHT, opór w postaci Bearish FVG, niedawny duży ruch, itp.).

## 6. Podsumowanie i kolejność wejść
Konkretna lista priorytetów (1–5) z jednozdaniowym uzasadnieniem dla każdej pozycji. Który ticker wchodzi pierwszy i dlaczego?

Używaj konkretnych danych liczbowych z listy. Odpowiedź NIE stanowi porady inwestycyjnej.`;
}

// ── Wywołanie API z SSE streamingiem ─────────────────────────
async function runAiAnalysis() {
  const key = (document.getElementById('ai-key-input') || {}).value.trim();
  if (!key) { showAiStatus('error', 'Wpisz klucz Anthropic API (sk-ant-...)'); return; }
  if (!SIGNALS_DATA || SIGNALS_DATA.length === 0) {
    showAiStatus('error', 'Ten skan nie zawiera sygnałów — brak danych do analizy.'); return;
  }

  const prompt = buildPrompt();
  const btn    = document.getElementById('ai-run-btn');
  const output = document.getElementById('ai-output');

  btn.disabled = true;
  btn.innerHTML = '<span class="ai-spin">⟳</span> Analizuję...';
  output.innerHTML = '<div class="ai-loading"><div class="ai-dots"><span></span><span></span><span></span></div>'
    + '<div style="margin-top:12px;font-size:13px">Claude analizuje ' + SIGNALS_DATA.length + ' sygnałów…</div></div>';

  try {
    const resp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': key,
        'anthropic-version': '2023-06-01',
        'anthropic-dangerous-direct-browser-access': 'true',
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 4000,
        stream: true,
        messages: [{ role: 'user', content: prompt }],
      }),
    });

    if (!resp.ok) {
      let msg = `HTTP ${resp.status}`;
      try { const e = await resp.json(); msg = e.error?.message || msg; } catch {}
      throw new Error(msg);
    }

    output.innerHTML = '<div class="ai-result" id="ai-result-inner"></div>';
    const el = document.getElementById('ai-result-inner');
    let fullText = '';
    let buf = '';

    const reader  = resp.body.getReader();
    const decoder = new TextDecoder();

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      const lines = buf.split('\\n');
      buf = lines.pop();          // niedokończona linia – trzymamy w buforze
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        const raw = line.slice(6).trim();
        if (raw === '[DONE]') continue;
        try {
          const parsed = JSON.parse(raw);
          if (parsed.type === 'content_block_delta' && parsed.delta?.type === 'text_delta') {
            fullText += parsed.delta.text;
            el.innerHTML = renderMd(fullText) + '<span class="ai-cursor">▋</span>';
            el.scrollIntoView({ block: 'end', behavior: 'smooth' });
          }
        } catch {}
      }
    }
    el.innerHTML = renderMd(fullText);
    showAiStatus('ok', '✓ Analiza zakończona');

  } catch (err) {
    output.innerHTML = '<div class="ai-error">❌ ' + err.message + '<br><small>Sprawdź klucz API i połączenie z internetem.</small></div>';
  } finally {
    btn.disabled = false;
    btn.innerHTML = '✦ Analizuj z Claude';
  }
}

function showAiStatus(type, msg) {
  const el = document.getElementById('ai-status');
  if (!el) return;
  el.className = 'ai-status ' + type;
  el.textContent = msg;
  el.style.display = 'block';
  if (type === 'ok') setTimeout(() => { el.style.display = 'none'; }, 4000);
}

// ── Prosty renderer Markdown → HTML ──────────────────────────
function renderMd(raw) {
  let t = raw
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/^## (.+)$/gm,  '<h3 class="ai-h2">$1</h3>')
    .replace(/^### (.+)$/gm, '<h4 class="ai-h3">$1</h4>')
    .replace(/\\*\\*(.+?)\\*\\*/g, '<strong>$1</strong>')
    .replace(/\\*(.+?)\\*/g,  '<em>$1</em>')
    .replace(/`(.+?)`/g,      '<code class="ai-code">$1</code>');

  // Listy: zbieramy kolejne linie z "-" i "1." w bloki <ul>/<ol>
  const lines = t.split('\\n');
  const out = [];
  let inUl = false, inOl = false;
  for (const ln of lines) {
    const ulM = ln.match(/^- (.+)$/);
    const olM = ln.match(/^(\\d+)\\. (.+)$/);
    if (ulM) {
      if (inOl) { out.push('</ol>'); inOl = false; }
      if (!inUl) { out.push('<ul class="ai-ul">'); inUl = true; }
      out.push(`<li>${ulM[1]}</li>`);
    } else if (olM) {
      if (inUl) { out.push('</ul>'); inUl = false; }
      if (!inOl) { out.push('<ol class="ai-ol">'); inOl = true; }
      out.push(`<li><span class="ai-num">${olM[1]}.</span> ${olM[2]}</li>`);
    } else {
      if (inUl) { out.push('</ul>'); inUl = false; }
      if (inOl) { out.push('</ol>'); inOl = false; }
      out.push(ln === '' ? '<div class="ai-gap"></div>' : `<p class="ai-p">${ln}</p>`);
    }
  }
  if (inUl) out.push('</ul>');
  if (inOl) out.push('</ol>');
  return out.join('\\n');
}
"""
    )


if __name__ == "__main__":
    run_screener()
