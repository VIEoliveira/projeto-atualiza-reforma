import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def handler(request):
    print("Handler iniciado.")

    WEBHOOK = "https://hook.us2.make.com/9v46zbanehc2m84vjk1scd4718xwhdmb"

    SITES = [
        {
            "url": "https://noticias.iob.com.br/reforma-tributaria/",
            "selector": ".td-module-title a",
            "fonte": "IOB Reformas"
        },
        {
            "url": "https://www.contabeis.com.br/noticias/",
            "selector": ".noticia a",
            "fonte": "Portal Contábeis"
        },
        {
            "url": "https://www.jornalcontabil.com.br/category/economia/",
            "selector": ".td-module-title a",
            "fonte": "Jornal Contábil"
        }
    ]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    todas = []

    for site in SITES:
        try:
            print(f"Extraindo de {site['fonte']}...")
            r = requests.get(site["url"], headers=headers)
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
            todas.append({
                "titulo": f"Erro ao processar {site['fonte']}",
                "link": None,
                "fonte": str(e)
            })

    print("Total extraído:", len(todas))
    print("Enviando para o Make...")

    requests.post(WEBHOOK, json={"noticias": todas})

    print("Finalizado.")
    return {
        "statusCode": 200,
        "body": "Scraping enviado com sucesso ao Make!"
    }
