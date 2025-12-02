from http.server import BaseHTTPRequestHandler
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import json

WEBHOOK = "https://hook.us2.make.com/9v46zbanehc2m84vjk1scd4718xwhdmb"

SITES = [
    {
        "url": "https://noticias.iob.com.br/reforma-tributaria/",
        "selector": ".td-module-title a",
        "fonte": "IOB Reformas"
    },
    {
        "url": "https://www.legisweb.com.br/noticias/?termo=Reforma+Tribut%E1ria&assunto=&acao=Buscar",
        "selector": ".noticia a",
        "fonte": "LegisWeb"
    },
    {
        "url": "https://www.reformatributaria.com/ultimas-noticias/",
        "selector": ".td-module-title a",
        "fonte": "Portal Reforma"
    }
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}


def coletar_noticias():
    print("Iniciando scraping...")

    todas = []

    for site in SITES:
        try:
            print(f"Extraindo de {site['fonte']}...")
            r = requests.get(site["url"], headers=HEADERS, timeout=15)
            r.raise_for_status()

            soup = BeautifulSoup(r.text, "html.parser")

            for item in soup.select(site["selector"]):
                titulo = item.get_text(strip=True)
                link = item.get("href")

                if link and link.startswith("/"):
                    link = urljoin(site["url"], link)

                todas.append({
                    "titulo": titulo,
                    "link": link,
                    "fonte": site["fonte"]
                })

        except Exception as e:
            print(f"Erro ao processar {site['fonte']}: {e}")
            todas.append({
                "titulo": f"Erro ao processar {site['fonte']}",
                "link": None,
                "fonte": str(e)
            })

    print("Total extraído:", len(todas))
    return todas


class handler(BaseHTTPRequestHandler):
    def _executar(self):
        try:
            noticias = coletar_noticias()

            print("Enviando para o Make...")
            try:
                resp = requests.post(WEBHOOK, json={"noticias": noticias}, timeout=15)
                print("Resposta do Make:", resp.status_code, resp.text[:200])
            except Exception as e:
                print("Erro ao chamar o webhook do Make:", e)

            body = {
                "mensagem": "Scraping enviado com sucesso ao Make!",
                "quantidade_noticias": len(noticias)
            }

            body_str = json.dumps(body, ensure_ascii=False)

            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(body_str.encode("utf-8"))

        except Exception as e:
            # Em caso de erro geral
            print("Erro geral na função:", e)
            body = {"erro": str(e)}
            body_str = json.dumps(body, ensure_ascii=False)

            self.send_response(500)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(body_str.encode("utf-8"))

    # Aceita tanto GET quanto POST
    def do_GET(self):
        self._executar()

    def do_POST(self):
        self._executar()
