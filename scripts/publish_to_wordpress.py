"""
publish_to_wordpress.py

Wysyla najnowsze wyniki screenera na strone zhelektro.pl przez endpoint MCP
wtyczki Easy MCP AI (ten sam mechanizm, ktorego Claude uzywa do zarzadzania
ta strona). Omija to problem z Application Passwords, ktorych nie udalo sie
odblokowac (prawdopodobnie is_ssl() na hostingu zwraca false za proxy/LiteSpeed
-- to wymagaloby zmian w konfiguracji serwera, do ktorej nie masz latwego dostepu).

WYMAGANA ZMIENNA SRODOWISKOWA (ustaw jako GitHub Secret):
  WP_API_TOKEN  - token z: wp-admin -> Easy MCP AI -> API Tokens -> Create New Token
                  -> uzytkownik: Administrator
                  -> Allowed Tools: zostaw puste, albo ogranicz do "wp_get_page,wp_update_page"
                  -> skopiuj wartosc zaczynajaca sie od "wpmcp_" (pokazana tylko raz!)

PODLACZENIE DO screener.py:
  from publish_to_wordpress import publish_results

  results = [
      {"ticker": "AGI", "price": 33.15, "smi": -65.89, "strefa": "OVERSOLD",
       "sygnal": "Strong BUY", "tech": 11},
      ...
  ]
  publish_results(results)

Dostosuj klucze slownika do rzeczywistej struktury danych w Twoim screener.py.

UWAGA: ta implementacja mowi standardowym protokolem MCP (JSON-RPC przez HTTP)
zgodnie z dokumentacja Easy MCP AI. Nie mam mozliwosci przetestowac jej na
Twoim koncie/tokenie -- jesli pierwszy przebieg w GitHub Actions sie wysypie,
log pokaze pelna tresc odpowiedzi serwera, co powinno wystarczyc do poprawki.
"""

import os
import re
import json
import itertools
import requests

MCP_URL = "https://zhelektro.pl/wp-json/easy-mcp-ai/v1/mcp"
WP_API_TOKEN = os.environ["WP_API_TOKEN"]

# 328 = "Strona glowna (projekt)" -- zmien na 20, gdy strona glowna zostanie podmieniona
PAGE_ID_HOMEPAGE = 328
PAGE_ID_RESULTS = 330

_id_counter = itertools.count(1)

SIGNAL_COLORS = {
    "Strong BUY": ("#35D07F", "#163325"),
    "BUY": ("#3DDAD7", "#163333"),
    "Turning Up": ("#FFB020", "#3A2E10"),
    "Bearish": ("#FF8A3D", "#3A2418"),
}


class MCPClient:
    """Minimalny klient JSON-RPC dla endpointu MCP Easy MCP AI (Streamable HTTP)."""

    def __init__(self, url, token):
        self.url = url
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        })
        self._initialize()

    def _post(self, payload):
        resp = self.session.post(self.url, json=payload, timeout=30)
        if not resp.ok:
            raise RuntimeError(
                f"HTTP {resp.status_code} z endpointu MCP. Tresc odpowiedzi:\n{resp.text[:2000]}"
            )
        session_id = resp.headers.get("Mcp-Session-Id")
        if session_id:
            self.session.headers["Mcp-Session-Id"] = session_id
        return resp

    def _initialize(self):
        self._post({
            "jsonrpc": "2.0",
            "id": next(_id_counter),
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "screener-publisher", "version": "1.0"},
            },
        })
        self._post({"jsonrpc": "2.0", "method": "notifications/initialized"})

    def call_tool(self, name, arguments):
        resp = self._post({
            "jsonrpc": "2.0",
            "id": next(_id_counter),
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        })
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"Blad MCP przy wywolaniu {name}: {data['error']}")
        result = data["result"]
        content = result.get("content", [])
        if content and content[0].get("type") == "text":
            try:
                return json.loads(content[0]["text"])
            except (ValueError, KeyError):
                return content[0]["text"]
        return result


def _signal_badge(sygnal):
    color, bg = SIGNAL_COLORS.get(sygnal, ("#8A93A0", "#1d222a"))
    return (
        f'<span style="background:{bg};color:{color};padding:2px 8px;'
        f'border-radius:4px;font-size:12px;font-weight:600;">{sygnal}</span>'
    )


def _build_top2_html(top2):
    cards = []
    for r in top2:
        cards.append(f'''
        <div style="background:#161A20;border:1px solid #2A3038;border-radius:8px;padding:14px 16px;margin-bottom:10px;">
          <div style="display:flex;justify-content:space-between;align-items:center;">
            <span style="font-family:ui-monospace,'SF Mono',Consolas,monospace;font-weight:600;font-size:15px;">{r['ticker']}</span>
            <span style="font-family:ui-monospace,'SF Mono',Consolas,monospace;">${r['price']:.2f}</span>
          </div>
          <div style="margin-top:6px;">{_signal_badge(r['sygnal'])}</div>
        </div>''')
    return "\n".join(cards)


def _build_table_html(results):
    rows = []
    for r in results:
        rows.append(f'''
        <tr style="border-bottom:1px solid #1d222a;">
          <td style="padding:10px 8px;font-family:ui-monospace,'SF Mono',Consolas,monospace;font-weight:600;">{r['ticker']}</td>
          <td style="padding:10px 8px;text-align:right;font-family:ui-monospace,'SF Mono',Consolas,monospace;">${r['price']:.2f}</td>
          <td style="padding:10px 8px;text-align:right;font-family:ui-monospace,'SF Mono',Consolas,monospace;">{r['smi']:.2f}</td>
          <td style="padding:10px 8px;">{r['strefa']}</td>
          <td style="padding:10px 8px;">{_signal_badge(r['sygnal'])}</td>
          <td style="padding:10px 8px;text-align:right;font-family:ui-monospace,'SF Mono',Consolas,monospace;">{r['tech']}</td>
        </tr>''')
    return (
        '<table style="width:100%;border-collapse:collapse;font-size:14px;">'
        '<thead><tr style="border-bottom:1px solid #2A3038;">'
        '<th style="text-align:left;padding:10px 8px;color:#8A93A0;">Ticker</th>'
        '<th style="text-align:right;padding:10px 8px;color:#8A93A0;">Cena</th>'
        '<th style="text-align:right;padding:10px 8px;color:#8A93A0;">SMI</th>'
        '<th style="text-align:left;padding:10px 8px;color:#8A93A0;">Strefa</th>'
        '<th style="text-align:left;padding:10px 8px;color:#8A93A0;">Sygnal</th>'
        '<th style="text-align:right;padding:10px 8px;color:#8A93A0;">Tech</th>'
        '</tr></thead><tbody>' + "\n".join(rows) + '</tbody></table>'
    )


def _replace_between_markers(content, marker, new_html):
    pattern = re.compile(rf"(<!--{marker}_START-->)(.*?)(<!--{marker}_END-->)", re.DOTALL)
    if not pattern.search(content):
        raise RuntimeError(
            f"Nie znaleziono znacznikow {marker}_START/{marker}_END na stronie -- "
            "sprawdz, czy nie zostaly przypadkiem usuniete w edytorze WordPressa."
        )
    return pattern.sub(rf"\1\n{new_html}\n\3", content)


def publish_results(results):
    """results: lista slownikow z kluczami ticker, price, smi, strefa, sygnal, tech."""
    client = MCPClient(MCP_URL, WP_API_TOKEN)

    ranked = sorted(results, key=lambda r: r["tech"], reverse=True)
    top2 = ranked[:2]

    homepage = client.call_tool("wp_get_page", {"page_id": PAGE_ID_HOMEPAGE})
    new_homepage_content = _replace_between_markers(
        homepage["content"], "TOP2", _build_top2_html(top2)
    )
    client.call_tool("wp_update_page", {
        "page_id": PAGE_ID_HOMEPAGE, "content": new_homepage_content,
    })

    results_page = client.call_tool("wp_get_page", {"page_id": PAGE_ID_RESULTS})
    new_results_content = _replace_between_markers(
        results_page["content"], "TABLE", _build_table_html(ranked)
    )
    client.call_tool("wp_update_page", {
        "page_id": PAGE_ID_RESULTS, "content": new_results_content,
    })

    print(f"Zaktualizowano strone glowna (TOP {len(top2)}) i pelna tabele ({len(ranked)} wierszy).")


if __name__ == "__main__":
    example = [
        {"ticker": "AGI", "price": 33.15, "smi": -65.89, "strefa": "OVERSOLD", "sygnal": "Strong BUY", "tech": 11},
        {"ticker": "ITRG", "price": 2.57, "smi": -50.48, "strefa": "OVERSOLD", "sygnal": "Strong BUY", "tech": 11},
    ]
    publish_results(example)
