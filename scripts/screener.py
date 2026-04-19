"""
Stock Screener – Stochastic RSI Strategy
Kryteria: USA + Europa | cena < 50 USD/EUR | rosnące Revenue i Net Income QoQ
Sygnał wejścia: bullish cross Stochastic RSI na interwale tygodniowym
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

MAX_PRICE = 50
DELAY     = 0.25   # sekundy między requestami
OUTPUT_DIR = "results"

# ══════════════════════════════════════════════════════════════
#  POBIERANIE TICKERÓW
# ══════════════════════════════════════════════════════════════

def get_sp500():
    """Pobiera S&P 500 z iShares ETF holdings (IVV) - dziala z GitHub Actions"""
    try:
        url = ("https://www.ishares.com/us/products/239726/ISHARES-CORE-SP-500-ETF/"
               "1467271812596.ajax?fileType=csv&fileName=IVV_holdings&dataType=fund")
        r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        df = pd.read_csv(StringIO(r.text), skiprows=9)
        df = df[df["Asset Class"] == "Equity"]
        tickers = df["Ticker"].dropna().str.strip().str.replace(".", "-", regex=False).tolist()
        print(f"  S&P 500 (iShares IVV): {len(tickers)} spolok")
        return tickers
    except Exception as e:
        print(f"  S&P 500 blad: {e}")
        return []

def get_sp600():
    """Pobiera S&P 600 z iShares ETF holdings (IJR) - dziala z GitHub Actions"""
    try:
        url = ("https://www.ishares.com/us/products/239774/ISHARES-CORE-SP-SMALLCAP-ETF/"
               "1467271812596.ajax?fileType=csv&fileName=IJR_holdings&dataType=fund")
        r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
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
        r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        df = pd.read_csv(StringIO(r.text), skiprows=9)
        df = df[df["Asset Class"] == "Equity"]
        tickers = df["Ticker"].dropna().str.strip().tolist()
        print(f"  Russell 2000: {len(tickers)} spółek")
        return tickers
    except Exception as e:
        print(f"  Russell 2000 błąd: {e}")
        return []

def get_european_indices():
    """
    Statyczna lista tickerów europejskich z poprawnymi sufiksami Yahoo Finance.
    Używamy statycznej listy zamiast scrapingu Wikipedii,
    bo GitHub Actions jest blokowany przez Wikipedia (403).
    """
    # DAX 40 (.DE)
    dax = [
        "ADS.DE","AIR.DE","ALV.DE","BAS.DE","BAYN.DE","BEI.DE","BMW.DE","BNR.DE",
        "CON.DE","1COV.DE","DHER.DE","DB1.DE","DBK.DE","DHL.DE","DTE.DE","EOAN.DE",
        "FRE.DE","FME.DE","HEI.DE","HEN3.DE","IFX.DE","LIN.DE","MBG.DE","MRK.DE",
        "MTX.DE","MUV2.DE","PAH3.DE","POWR.DE","QGEN.DE","RHM.DE","RWE.DE","SAP.DE",
        "SHL.DE","SIE.DE","SY1.DE","VNA.DE","VOW3.DE","ZAL.DE","PUM.DE","ENR.DE",
    ]
    # CAC 40 (.PA)
    cac = [
        "AC.PA","ACA.PA","AI.PA","AIR.PA","ALO.PA","MT.PA","ATO.PA","CS.PA","BNP.PA",
        "EN.PA","CAP.PA","CA.PA","AXA.PA","DSY.PA","EDEN.PA","EL.PA","ERF.PA","EDF.PA",
        "ENGI.PA","FP.PA","KER.PA","LR.PA","LHN.PA","MC.PA","ML.PA","ORA.PA","RI.PA",
        "PUB.PA","RNO.PA","SAF.PA","SGO.PA","SAN.PA","SU.PA","GLE.PA","STLAM.PA",
        "STM.PA","TEP.PA","HO.PA","URW.PA","VIE.PA","DG.PA","VIV.PA","WLN.PA",
    ]
    # FTSE 100 (.L)
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
    # AEX (.AS)
    aex = [
        "ABN.AS","ADYEN.AS","AGN.AS","AH.AS","AKZA.AS","MT.AS","ASML.AS","ASR.AS",
        "BESI.AS","DSMF.AS","EXOR.AS","HEIA.AS","IMCD.AS","INGA.AS","JUST.AS",
        "KPN.AS","NN.AS","PHIA.AS","PRX.AS","RAND.AS","REN.AS","SHELL.AS","SBM.AS",
        "URW.AS","UNA.AS","VPK.AS","WKL.AS",
    ]
    # IBEX 35 (.MC)
    ibex = [
        "ACS.MC","ACX.MC","AMS.MC","ANA.MC","BBVA.MC","BKT.MC","CABK.MC","CLNX.MC",
        "COL.MC","ELE.MC","ENG.MC","FDR.MC","FER.MC","GRF.MC","IAG.MC","IBE.MC",
        "IDR.MC","ITX.MC","LOG.MC","MAP.MC","MEL.MC","MRL.MC","MTS.MC","NTGY.MC",
        "RED.MC","REE.MC","REP.MC","ROVI.MC","SAB.MC","SAN.MC","SGRE.MC","SOL.MC",
        "TEF.MC","UNI.MC","VIS.MC",
    ]
    # SMI (.SW)
    smi = [
        "ABBN.SW","ADEN.SW","ALC.SW","CSGN.SW","GEBN.SW","GIVN.SW","CFR.SW",
        "HOLN.SW","LONN.SW","NESN.SW","NOVN.SW","ROG.SW","SANN.SW","SCMN.SW",
        "SGSN.SW","SLHN.SW","SRENH.SW","UBSG.SW","ZURN.SW",
    ]
    # FTSE MIB (.MI)
    mib = [
        "A2A.MI","AMP.MI","ATL.MI","AZM.MI","BMED.MI","BMPS.MI","BZU.MI","CPR.MI",
        "DIA.MI","ENEL.MI","ENI.MI","EXOR.MI","FCA.MI","FBK.MI","G.MI","HER.MI",
        "ISP.MI","IVG.MI","LDO.MI","MB.MI","MONC.MI","PIRC.MI","PRY.MI","PST.MI",
        "REC.MI","SRG.MI","STM.MI","TEN.MI","TIT.MI","TRN.MI","UCG.MI","UNI.MI",
    ]
    # OMX Stockholm 30 (.ST)
    omx = [
        "ABB.ST","ALFA.ST","ASSA-B.ST","AZN.ST","ATCO-A.ST","BOL.ST","ERIC-B.ST",
        "ESSITY-B.ST","EVO.ST","GETI-B.ST","HEXA-B.ST","HM-B.ST","HUFV-A.ST",
        "INVE-B.ST","KINV-B.ST","NDA-SE.ST","SAND.ST","SCA-B.ST","SEB-A.ST",
        "SECU-B.ST","SKA-B.ST","SKF-B.ST","SSAB-A.ST","SHB-A.ST","SWED-A.ST",
        "SWMA.ST","TEL2-B.ST","TELIA.ST","VOLV-B.ST","VOLCAR-B.ST",
    ]
    # OBX Norway (.OL)
    obx = [
        "AKERBP.OL","AKSO.OL","AKER.OL","AMSC.OL","AUTO.OL","BAKKA.OL","DNB.OL",
        "EQNR.OL","FRO.OL","GOGL.OL","MOWI.OL","NEL.OL","NHY.OL","NSKOG.OL",
        "ORK.OL","PGS.OL","REC.OL","SALM.OL","SCHA.OL","SDRL.OL","SNOG.OL",
        "STB.OL","SUBC.OL","TEL.OL","TOM.OL","TGS.OL","VAR.OL","WILS.OL","YAR.OL",
    ]
    # BEL 20 (.BR)
    bel = [
        "ABI.BR","ACKB.BR","AGS.BR","APAM.BR","ARGX.BR","COLR.BR","D5MT.BR",
        "EKTA-B.BR","GBL.BR","GLPG.BR","KBC.BR","MELE.BR","ONTEX.BR","PROX.BR",
        "SOLB.BR","TNET.BR","UCB.BR","UMI.BR","WDP.BR",
    ]
    # WIG20 (.WA)
    wig = [
        "ALE.WA","CCC.WA","CDR.WA","CPS.WA","DNP.WA","JSW.WA","KGH.WA","KRU.WA",
        "LPP.WA","MBK.WA","OPL.WA","PCO.WA","PEO.WA","PGE.WA","PKN.WA","PKO.WA",
        "PZU.WA","SPL.WA","TPE.WA","XTB.WA",
    ]

    all_tickers = dax + cac + ftse + aex + ibex + smi + mib + omx + obx + bel + wig
    all_tickers = list(set(all_tickers))
    print(f"  EU statyczna lista: {len(all_tickers)} tickerow")
    print(f"    DAX:{len(dax)} CAC:{len(cac)} FTSE:{len(ftse)} AEX:{len(aex)} IBEX:{len(ibex)}")
    print(f"    SMI:{len(smi)} MIB:{len(mib)} OMX:{len(omx)} OBX:{len(obx)} BEL:{len(bel)} WIG:{len(wig)}")
    return all_tickers


# ══════════════════════════════════════════════════════════════
#  ANALIZA TECHNICZNA
# ══════════════════════════════════════════════════════════════

def stoch_rsi(close, rsi_p=14, stoch_p=14, k_smooth=3, d_smooth=3):
    delta = close.diff()
    gain  = delta.clip(lower=0).rolling(rsi_p).mean()
    loss  = (-delta.clip(upper=0)).rolling(rsi_p).mean()
    rs    = gain / (loss + 1e-10)
    rsi   = 100 - (100 / (1 + rs))
    lo    = rsi.rolling(stoch_p).min()
    hi    = rsi.rolling(stoch_p).max()
    k     = 100 * (rsi - lo) / (hi - lo + 1e-10)
    k     = k.rolling(k_smooth).mean()
    d     = k.rolling(d_smooth).mean()
    return k, d

def bullish_cross(k, d):
    """K przecina D od dołu (bullish cross w ostatnich 2 świecach)"""
    if len(k) < 3:
        return False
    return (float(k.iloc[-2]) < float(d.iloc[-2])) and (float(k.iloc[-1]) > float(d.iloc[-1]))

# ══════════════════════════════════════════════════════════════
#  ANALIZA FUNDAMENTALNA
# ══════════════════════════════════════════════════════════════

def check_fundamentals(tkr_obj, min_annual_rev_growth=0.15):
    """
    Sprawdza kryteria fundamentalne:
    - Revenue rosnace QoQ (ostatni kwartal vs poprzedni)
    - Net Income rosnace QoQ
    - Roczny wzrost przychodow > min_annual_rev_growth (domyslnie 15%)
    Zwraca: (rev_up, earn_up, rev_vals, earn_vals, annual_rev_growth)
    """
    try:
        q = tkr_obj.quarterly_financials
        if q is None or q.empty:
            return False, False, None, None, None

        rev_up = earn_up = False
        rev_vals = earn_vals = None
        annual_rev_growth = None

        if "Total Revenue" in q.index:
            rev = q.loc["Total Revenue"].dropna()

            # QoQ: ostatni kwartal vs poprzedni
            if len(rev) >= 2:
                rev_up   = float(rev.iloc[0]) > float(rev.iloc[1])
                rev_vals = (round(float(rev.iloc[1])/1e6, 1), round(float(rev.iloc[0])/1e6, 1))

            # YoY: suma ostatnich 4 kwartalow vs 4 poprzednich
            if len(rev) >= 8:
                ttm_curr = float(rev.iloc[0:4].sum())
                ttm_prev = float(rev.iloc[4:8].sum())
                if ttm_prev > 0:
                    annual_rev_growth = (ttm_curr - ttm_prev) / ttm_prev
            # Fallback: ostatni kwartal vs ten sam kwartal rok temu
            elif len(rev) >= 5:
                curr = float(rev.iloc[0])
                prev_year = float(rev.iloc[4])
                if prev_year > 0:
                    annual_rev_growth = (curr - prev_year) / prev_year

        if "Net Income" in q.index:
            net = q.loc["Net Income"].dropna()
            if len(net) >= 2:
                earn_up   = float(net.iloc[0]) > float(net.iloc[1])
                earn_vals = (round(float(net.iloc[1])/1e6, 1), round(float(net.iloc[0])/1e6, 1))

        # Filtr rocznego wzrostu przychodow
        if annual_rev_growth is None:
            rev_annual_ok = False  # brak danych = odrzuc
        else:
            rev_annual_ok = annual_rev_growth >= min_annual_rev_growth

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
    print("=" * 60)

    print("\n[1/5] Pobieranie list spółek...")
    usa_tickers = list(set(get_sp500() + get_sp600() + get_russell2000()))
    eu_tickers  = list(set(get_european_indices()))
    all_tickers = [(t, "USA") for t in usa_tickers] + [(t, "EU") for t in eu_tickers]
    print(f"\nŁącznie: {len(all_tickers)} spółek ({len(usa_tickers)} USA, {len(eu_tickers)} EU)")

    print("\n[2/5] Analiza spółek...")
    results   = []
    signals   = []
    skipped   = 0
    errors    = 0

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

            hist = tkr.history(period="2y", interval="1wk")["Close"].dropna()
            if len(hist) < 35:
                skipped += 1
                continue

            k, d    = stoch_rsi(hist)
            signal  = bullish_cross(k, d)
            k_val   = round(float(k.iloc[-1]), 1)
            d_val   = round(float(d.iloc[-1]), 1)

            try:
                info    = tkr.info
                name    = info.get("shortName", symbol)
                sector  = info.get("sector", "—")
                country = info.get("country", "—")
            except:
                name    = symbol
                sector  = "—"
                country = "—"

            annual_growth_pct = round(annual_growth * 100, 1) if annual_growth is not None else None

            row = {
                "ticker":        symbol,
                "name":          name,
                "market":        market,
                "country":       country,
                "sector":        sector,
                "price":         round(price, 2),
                "currency":      currency,
                "stoch_k":       k_val,
                "stoch_d":       d_val,
                "k_above_d":     k_val > d_val,
                "signal":        signal,
                "rev_prev":      rev_vals[0] if rev_vals else None,
                "rev_curr":      rev_vals[1] if rev_vals else None,
                "earn_prev":     earn_vals[0] if earn_vals else None,
                "earn_curr":     earn_vals[1] if earn_vals else None,
                "rev_yoy_pct":   annual_growth_pct,
                "scanned_at":    datetime.now().isoformat(),
            }
            results.append(row)
            growth_str = f"+{annual_growth_pct}% YoY" if annual_growth_pct else ""
            if signal:
                signals.append(row)
                print(f"  SYGNAŁ [{i+1}] {symbol:10s} {market} | {price:6.2f} {currency} | K={k_val} D={d_val} | {growth_str}")
            else:
                print(f"  ok     [{i+1}] {symbol:10s} {market} | {price:6.2f} {currency} | K={k_val} D={d_val} | {growth_str}")

            time.sleep(DELAY)

        except KeyboardInterrupt:
            print("\nPrzerwano przez użytkownika.")
            break
        except Exception as e:
            errors += 1

    elapsed = round((datetime.now() - start).total_seconds() / 60, 1)
    print(f"\n[3/5] Skanowanie zakończone w {elapsed} min")
    print(f"  Kandydaci: {len(results)} | Sygnały: {len(signals)} | Pominięto: {skipped} | Błędy: {errors}")

    # Zapis JSON
    print("\n[4/5] Zapis wyników...")
    meta = {
        "generated_at": datetime.now().isoformat(),
        "elapsed_min":  elapsed,
        "total_scanned": len(all_tickers),
        "candidates":   len(results),
        "signals":      len(signals),
        "skipped":      skipped,
        "errors":       errors,
    }
    with open(f"{OUTPUT_DIR}/meta.json", "w") as f:
        json.dump(meta, f, indent=2)
    with open(f"{OUTPUT_DIR}/results.json", "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    with open(f"{OUTPUT_DIR}/signals.json", "w") as f:
        json.dump(signals, f, indent=2, ensure_ascii=False)

    # Zapis CSV
    if results:
        pd.DataFrame(results).to_csv(f"{OUTPUT_DIR}/results.csv", index=False)
    if signals:
        pd.DataFrame(signals).to_csv(f"{OUTPUT_DIR}/signals.csv", index=False)

    print("[5/5] Generowanie raportu HTML...")
    generate_html(meta, results, signals)
    print(f"\nGotowe! Wyniki w katalogu: {OUTPUT_DIR}/")
    return signals

# ══════════════════════════════════════════════════════════════
#  GENEROWANIE RAPORTU HTML
# ══════════════════════════════════════════════════════════════

def generate_html(meta, results, signals):
    dt = datetime.fromisoformat(meta["generated_at"]).strftime("%d.%m.%Y %H:%M")

    def rows_html(data, show_signal_badge=False):
        if not data:
            return "<tr><td colspan='9' style='text-align:center;color:#888;padding:2rem'>Brak wyników</td></tr>"
        html = ""
        for r in data:
            sig_badge = ""
            if show_signal_badge and r.get("signal"):
                sig_badge = '<span class="badge-signal">CROSS</span>'
            rev_str  = f"{r['rev_prev']} → {r['rev_curr']} M" if r.get("rev_curr") else "—"
            earn_str = f"{r['earn_prev']} → {r['earn_curr']} M" if r.get("earn_curr") else "—"
            k_cls    = "k-above" if r["k_above_d"] else "k-below"
            html += f"""
            <tr>
              <td><span class="ticker">{r['ticker']}</span>{sig_badge}</td>
              <td class="name-col">{r['name']}</td>
              <td><span class="badge-{'usa' if r['market']=='USA' else 'eu'}">{r['market']}</span></td>
              <td>{r['sector']}</td>
              <td class="num">{r['price']} {r['currency']}</td>
              <td class="num {k_cls}">{r['stoch_k']}</td>
              <td class="num">{r['stoch_d']}</td>
              <td class="num">{rev_str}</td>
              <td class="num">{earn_str}</td>
            </tr>"""
        return html

    signal_rows  = rows_html(signals, show_signal_badge=True)
    all_rows     = rows_html(sorted(results, key=lambda x: -x["stoch_k"]))
    signal_count = meta["signals"]
    cand_count   = meta["candidates"]
    scan_count   = meta["total_scanned"]
    elapsed      = meta["elapsed_min"]

    # Budujemy karty sygnałów osobno (unikamy zagnieżdżonych f-stringów)
    if not signals:
        signals_panel_html = "<div class='empty'>Brak sygnałów w tym skanie</div>"
    else:
        cards = ""
        for r in signals:
            market_cls = "usa" if r["market"] == "USA" else "eu"
            rev_str    = str(r["rev_prev"]) + " &rarr; " + str(r["rev_curr"]) + " M" if r.get("rev_curr") else "&mdash;"
            earn_str   = str(r["earn_prev"]) + " &rarr; " + str(r["earn_curr"]) + " M" if r.get("earn_curr") else "&mdash;"
            cards += (
                '<div class="signal-card">'
                '<div style="display:flex;justify-content:space-between;align-items:flex-start">'
                '<div><div class="sc-ticker">' + r["ticker"] + '</div>'
                '<div class="sc-name">' + r["name"] + '</div></div>'
                '<span class="badge-' + market_cls + '">' + r["market"] + '</span>'
                '</div>'
                '<div class="sc-price">' + str(r["price"]) + ' ' + r["currency"] + '</div>'
                '<div class="sc-row"><span>Sektor</span><span>' + r["sector"] + '</span></div>'
                '<div class="sc-row"><span>Kraj</span><span>' + r["country"] + '</span></div>'
                '<div class="sc-row"><span>Revenue QoQ</span>'
                '<span style="color:var(--green)">' + rev_str + '</span></div>'
                '<div class="sc-row"><span>Net Income QoQ</span>'
                '<span style="color:var(--green)">' + earn_str + '</span></div>'
                '<div class="sc-stoch">'
                '<div class="sc-stoch-item"><div class="sc-stoch-label">Stoch K</div>'
                '<div class="sc-stoch-val green">' + str(r["stoch_k"]) + '</div></div>'
                '<div class="sc-stoch-item"><div class="sc-stoch-label">Stoch D</div>'
                '<div class="sc-stoch-val">' + str(r["stoch_d"]) + '</div></div>'
                '</div></div>'
            )
        signals_panel_html = (
            '<div class="signal-grid">' + cards + '</div>'
            '<div class="table-wrap"><table id="tbl-signals"><thead><tr>'
            '<th>Ticker</th><th>Nazwa</th><th>Rynek</th><th>Sektor</th>'
            '<th style="text-align:right">Cena</th><th style="text-align:right">K</th>'
            '<th style="text-align:right">D</th><th style="text-align:right">Revenue (M)</th>'
            '<th style="text-align:right">Net Inc. (M)</th>'
            '</tr></thead><tbody>' + signal_rows + '</tbody></table></div>'
        )

    html = f"""<!DOCTYPE html>
<html lang="pl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Stock Screener – {dt}</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@300;400;500&display=swap" rel="stylesheet">
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

  :root {{
    --bg:       #0a0e14;
    --bg2:      #111620;
    --bg3:      #1a2030;
    --border:   #1e2d45;
    --text:     #c8d8f0;
    --muted:    #4a6080;
    --accent:   #00c8ff;
    --green:    #00e599;
    --amber:    #ffb800;
    --red:      #ff4560;
    --usa:      #3b82f6;
    --eu:       #10b981;
  }}

  body {{
    background: var(--bg);
    color: var(--text);
    font-family: 'IBM Plex Sans', sans-serif;
    font-size: 14px;
    line-height: 1.6;
    min-height: 100vh;
  }}

  /* HEADER */
  .header {{
    border-bottom: 1px solid var(--border);
    padding: 2rem 2.5rem 1.5rem;
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    flex-wrap: wrap;
    gap: 1rem;
  }}
  .header-left h1 {{
    font-family: 'IBM Plex Mono', monospace;
    font-size: 22px;
    font-weight: 500;
    color: #fff;
    letter-spacing: -0.5px;
  }}
  .header-left h1 span {{ color: var(--accent); }}
  .header-left p {{
    font-size: 12px;
    color: var(--muted);
    margin-top: 4px;
    font-family: 'IBM Plex Mono', monospace;
  }}
  .criteria-pills {{
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    margin-top: 12px;
  }}
  .pill {{
    font-family: 'IBM Plex Mono', monospace;
    font-size: 11px;
    padding: 3px 10px;
    border: 1px solid var(--border);
    border-radius: 100px;
    color: var(--muted);
  }}
  .pill.active {{ border-color: var(--accent); color: var(--accent); }}

  /* STATS */
  .stats {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    gap: 1px;
    background: var(--border);
    border-bottom: 1px solid var(--border);
  }}
  .stat {{
    background: var(--bg);
    padding: 1.25rem 1.5rem;
  }}
  .stat-label {{
    font-size: 11px;
    font-family: 'IBM Plex Mono', monospace;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 1px;
  }}
  .stat-value {{
    font-size: 28px;
    font-weight: 300;
    color: #fff;
    margin-top: 4px;
    font-family: 'IBM Plex Mono', monospace;
  }}
  .stat-value.highlight {{ color: var(--accent); }}
  .stat-value.green {{ color: var(--green); }}
  .stat-sub {{
    font-size: 11px;
    color: var(--muted);
    margin-top: 2px;
  }}

  /* TABS */
  .tabs {{
    display: flex;
    border-bottom: 1px solid var(--border);
    padding: 0 2rem;
    gap: 0;
  }}
  .tab {{
    font-family: 'IBM Plex Mono', monospace;
    font-size: 12px;
    padding: 12px 20px;
    cursor: pointer;
    color: var(--muted);
    border-bottom: 2px solid transparent;
    margin-bottom: -1px;
    transition: all .15s;
    background: none;
    border-top: none;
    border-left: none;
    border-right: none;
  }}
  .tab:hover {{ color: var(--text); }}
  .tab.active {{ color: var(--accent); border-bottom-color: var(--accent); }}

  /* CONTENT */
  .content {{ padding: 1.5rem 2rem; }}
  .panel {{ display: none; }}
  .panel.active {{ display: block; }}

  /* SEARCH */
  .toolbar {{
    display: flex;
    gap: 10px;
    margin-bottom: 1rem;
    flex-wrap: wrap;
    align-items: center;
  }}
  .search-input {{
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 7px 12px;
    color: var(--text);
    font-family: 'IBM Plex Mono', monospace;
    font-size: 12px;
    width: 240px;
    outline: none;
  }}
  .search-input:focus {{ border-color: var(--accent); }}
  .filter-select {{
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 7px 12px;
    color: var(--text);
    font-family: 'IBM Plex Mono', monospace;
    font-size: 12px;
    outline: none;
    cursor: pointer;
  }}
  .count-label {{
    font-family: 'IBM Plex Mono', monospace;
    font-size: 11px;
    color: var(--muted);
    margin-left: auto;
  }}

  /* TABLE */
  .table-wrap {{ overflow-x: auto; }}
  table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }}
  thead th {{
    font-family: 'IBM Plex Mono', monospace;
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: var(--muted);
    text-align: left;
    padding: 10px 12px;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
    cursor: pointer;
    user-select: none;
  }}
  thead th:hover {{ color: var(--text); }}
  tbody tr {{
    border-bottom: 1px solid var(--border);
    transition: background .1s;
  }}
  tbody tr:hover {{ background: var(--bg2); }}
  td {{
    padding: 10px 12px;
    vertical-align: middle;
  }}
  .ticker {{
    font-family: 'IBM Plex Mono', monospace;
    font-weight: 500;
    color: #fff;
    font-size: 13px;
  }}
  .name-col {{
    max-width: 180px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    color: var(--muted);
    font-size: 12px;
  }}
  .num {{
    font-family: 'IBM Plex Mono', monospace;
    font-size: 12px;
    text-align: right;
  }}
  .k-above {{ color: var(--green); }}
  .k-below {{ color: var(--red); }}

  /* BADGES */
  .badge-usa {{
    display: inline-block;
    font-size: 10px;
    font-family: 'IBM Plex Mono', monospace;
    padding: 2px 7px;
    border-radius: 4px;
    background: rgba(59,130,246,.15);
    color: var(--usa);
    border: 1px solid rgba(59,130,246,.3);
  }}
  .badge-eu {{
    display: inline-block;
    font-size: 10px;
    font-family: 'IBM Plex Mono', monospace;
    padding: 2px 7px;
    border-radius: 4px;
    background: rgba(16,185,129,.15);
    color: var(--eu);
    border: 1px solid rgba(16,185,129,.3);
  }}
  .badge-signal {{
    display: inline-block;
    font-size: 9px;
    font-family: 'IBM Plex Mono', monospace;
    padding: 1px 6px;
    border-radius: 4px;
    background: rgba(0,200,255,.15);
    color: var(--accent);
    border: 1px solid rgba(0,200,255,.3);
    margin-left: 6px;
    vertical-align: middle;
    animation: pulse 2s infinite;
  }}
  @keyframes pulse {{
    0%, 100% {{ opacity: 1; }}
    50% {{ opacity: .5; }}
  }}

  /* SIGNAL CARDS */
  .signal-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
    gap: 12px;
    margin-bottom: 1.5rem;
  }}
  .signal-card {{
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1rem 1.25rem;
    position: relative;
    overflow: hidden;
  }}
  .signal-card::before {{
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, var(--accent), var(--green));
  }}
  .sc-ticker {{
    font-family: 'IBM Plex Mono', monospace;
    font-size: 18px;
    font-weight: 500;
    color: #fff;
  }}
  .sc-name {{
    font-size: 12px;
    color: var(--muted);
    margin-top: 2px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }}
  .sc-price {{
    font-family: 'IBM Plex Mono', monospace;
    font-size: 20px;
    font-weight: 300;
    color: var(--accent);
    margin: 10px 0 8px;
  }}
  .sc-row {{
    display: flex;
    justify-content: space-between;
    font-size: 12px;
    color: var(--muted);
    margin-top: 4px;
  }}
  .sc-row span:last-child {{
    font-family: 'IBM Plex Mono', monospace;
    color: var(--text);
  }}
  .sc-stoch {{
    display: flex;
    gap: 12px;
    margin-top: 10px;
    padding-top: 10px;
    border-top: 1px solid var(--border);
  }}
  .sc-stoch-item {{ flex: 1; }}
  .sc-stoch-label {{
    font-size: 10px;
    font-family: 'IBM Plex Mono', monospace;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: .5px;
  }}
  .sc-stoch-val {{
    font-family: 'IBM Plex Mono', monospace;
    font-size: 16px;
    font-weight: 500;
    margin-top: 2px;
  }}
  .sc-stoch-val.green {{ color: var(--green); }}

  /* EMPTY STATE */
  .empty {{
    text-align: center;
    padding: 4rem 2rem;
    color: var(--muted);
    font-family: 'IBM Plex Mono', monospace;
    font-size: 13px;
  }}
  .empty::before {{
    content: '//';
    display: block;
    font-size: 32px;
    margin-bottom: 1rem;
    color: var(--border);
  }}

  footer {{
    border-top: 1px solid var(--border);
    padding: 1rem 2rem;
    font-size: 11px;
    color: var(--muted);
    font-family: 'IBM Plex Mono', monospace;
    display: flex;
    justify-content: space-between;
    flex-wrap: wrap;
    gap: 8px;
  }}
</style>
</head>
<body>

<div class="header">
  <div class="header-left">
    <h1>STOCK <span>SCREENER</span></h1>
    <p>// generated {dt} &nbsp;|&nbsp; elapsed {elapsed} min</p>
    <div class="criteria-pills">
      <span class="pill active">USA + Europa</span>
      <span class="pill active">cena ≤ 50 USD/EUR</span>
      <span class="pill active">Revenue ↑ QoQ</span>
      <span class="pill active">Net Income ↑ QoQ</span>
      <span class="pill active">Stoch RSI cross 1W</span>
    </div>
  </div>
</div>

<div class="stats">
  <div class="stat">
    <div class="stat-label">Przeskanowano</div>
    <div class="stat-value">{scan_count}</div>
    <div class="stat-sub">spółek USA + EU</div>
  </div>
  <div class="stat">
    <div class="stat-label">Kandydaci</div>
    <div class="stat-value highlight">{cand_count}</div>
    <div class="stat-sub">spełnia wszystkie kryteria</div>
  </div>
  <div class="stat">
    <div class="stat-label">Sygnały CROSS</div>
    <div class="stat-value green">{signal_count}</div>
    <div class="stat-sub">bullish Stoch RSI cross</div>
  </div>
  <div class="stat">
    <div class="stat-label">Czas skanu</div>
    <div class="stat-value">{elapsed}</div>
    <div class="stat-sub">minut</div>
  </div>
</div>

<div class="tabs">
  <button class="tab active" onclick="switchTab('signals', this)">Sygnały CROSS ({signal_count})</button>
  <button class="tab" onclick="switchTab('all', this)">Wszyscy kandydaci ({cand_count})</button>
</div>

<div class="content">

  <!-- SIGNALS PANEL -->
  <div id="panel-signals" class="panel active">
    {signals_panel_html}
  </div>

  <!-- ALL PANEL -->
  <div id="panel-all" class="panel">
    <div class="toolbar">
      <input class="search-input" type="text" placeholder="szukaj tickera lub nazwy..." oninput="filterTable(this.value)" />
      <select class="filter-select" onchange="filterMarket(this.value)">
        <option value="">Wszystkie rynki</option>
        <option value="USA">USA</option>
        <option value="EU">EU</option>
      </select>
      <span class="count-label" id="row-count">{cand_count} wyników</span>
    </div>
    <div class="table-wrap">
      <table id="tbl-all">
        <thead>
          <tr>
            <th>Ticker</th><th>Nazwa</th><th>Rynek</th><th>Sektor</th>
            <th style="text-align:right">Cena</th>
            <th style="text-align:right">K</th>
            <th style="text-align:right">D</th>
            <th style="text-align:right">Revenue (M)</th>
            <th style="text-align:right">Net Inc. (M)</th>
          </tr>
        </thead>
        <tbody id="tbody-all">{all_rows}</tbody>
      </table>
    </div>
  </div>

</div>

<footer>
  <span>// stock-screener &nbsp;|&nbsp; dane: Yahoo Finance &nbsp;|&nbsp; Stoch RSI(14,14,3,3) 1W</span>
  <span>Nie stanowi porady inwestycyjnej</span>
</footer>

<script>
function switchTab(name, btn) {{
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  btn.classList.add('active');
  document.getElementById('panel-' + name).classList.add('active');
}}

let marketFilter = '';
function filterMarket(val) {{
  marketFilter = val;
  applyFilters();
}}
function filterTable(val) {{
  applyFilters(val);
}}
function applyFilters(search) {{
  const rows = document.querySelectorAll('#tbody-all tr');
  let visible = 0;
  const s = (search ?? document.querySelector('.search-input').value).toLowerCase();
  rows.forEach(row => {{
    const text = row.textContent.toLowerCase();
    const marketOk = !marketFilter || text.includes(marketFilter.toLowerCase());
    const searchOk = !s || text.includes(s);
    const show = marketOk && searchOk;
    row.style.display = show ? '' : 'none';
    if (show) visible++;
  }});
  document.getElementById('row-count').textContent = visible + ' wyników';
}}
</script>
</body>
</html>"""

    with open(f"{OUTPUT_DIR}/index.html", "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  Raport: {OUTPUT_DIR}/index.html")

if __name__ == "__main__":
    run_screener()
