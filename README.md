# Stock Screener – Stochastic RSI Strategy

Automatyczny screener akcji uruchamiany ręcznie przez GitHub Actions.
Wyniki publikowane jako interaktywny raport HTML na GitHub Pages.

## Kryteria selekcji

| Kryterium | Wartość |
|-----------|---------|
| Rynki | USA (S&P 500 + S&P 600 + Russell 2000) + Europa (DAX, CAC40, FTSE100, AEX, IBEX35, SMI, MIB, OMX30) |
| Maks. cena | 50 USD / EUR |
| Fundamenty | Rosnące Revenue QoQ + rosnący Net Income QoQ |
| Sygnał wejścia | Bullish cross Stochastic RSI (14,14,3,3) na interwale tygodniowym |

## Jak uruchomić

1. Wejdź w zakładkę **Actions** w repozytorium
2. Wybierz workflow **Stock Screener** z listy po lewej
3. Kliknij **Run workflow** (prawy górny róg)
4. Ustaw parametry (opcjonalnie) i kliknij **Run workflow**
5. Poczekaj ~45–90 minut na zakończenie skanu
6. Wyniki pojawią się w:
   - **Artifacts** – pliki CSV i JSON do pobrania
   - **GitHub Pages** – interaktywny raport HTML

## Parametry workflow

| Parametr | Domyślnie | Opis |
|----------|-----------|------|
| `max_price` | `50` | Maksymalna cena akcji |
| `delay` | `0.25` | Opóźnienie między requestami (s) |
| `markets` | `USA+EU` | Wybór rynków |

## Wyniki

Po każdym uruchomieniu generowane są:

```
results/
├── index.html        ← interaktywny raport (GitHub Pages)
├── signals.csv       ← spółki z sygnałem CROSS
├── results.csv       ← wszyscy kandydaci
├── signals.json      ← dane sygnałów (JSON)
├── results.json      ← wszystkie wyniki (JSON)
└── meta.json         ← metadane skanu
```

## Włączenie GitHub Pages

1. Przejdź do **Settings → Pages**
2. Source: **Deploy from a branch**
3. Branch: **gh-pages** / root
4. Zapisz – po pierwszym uruchomieniu raport będzie pod:
   `https://<twoja-nazwa>.github.io/<nazwa-repo>/`

## Instalacja lokalna

```bash
pip install -r requirements.txt
python scripts/screener.py
# Raport: results/index.html
```

## Disclaimer

Narzędzie służy wyłącznie do celów informacyjnych i edukacyjnych.
Nie stanowi porady inwestycyjnej. Inwestowanie wiąże się z ryzykiem utraty kapitału.
