from http.server import BaseHTTPRequestHandler
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import json

# FIX: datas/regex/util
from datetime import datetime
from zoneinfo import ZoneInfo
import re

# DEDUP: util de URL
from urllib.parse import urlsplit, urlunsplit, parse_qsl

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

# FIX: mapas/regex para várias formas de data
MESES = {
    # abreviados e por extenso
    "jan": 1, "janeiro": 1,
    "fev": 2, "fevereiro": 2,
    "mar": 3, "março": 3, "marco": 3,
    "abr": 4, "abril": 4,
    "mai": 5, "maio": 5,
    "jun": 6, "junho": 6,
    "jul": 7, "julho": 7,
    "ago": 8, "agosto": 8,
    "set": 9, "setembro": 9,
    "out": 10, "outubro": 10,
    "nov": 11, "novembro": 11,
    "dez": 12, "dezembro": 12,
}

RE_NUM = re.compile(r"\b([0-3]?\d)[/\.]([01]?\d)[/\.](\d{4})\b")  # dd/mm/aaaa, d/m/aaaa, dd.mm.aaaa
RE_ISO = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")               # aaaa-mm-dd
RE_MES = re.compile(
    r"\b([0-3]?\d)\s*(?:de\s+)?"
    r"(jan(?:eiro)?|fev(?:ereiro)?|mar(?:ço|co)?|abr(?:il)?|mai(?:o)?|jun(?:ho)?|"
    r"jul(?:ho)?|ago(?:sto)?|set(?:embro)?|out(?:ubro)?|nov(?:embro)?|dez(?:embro)?)"
    r"(?:\s*de)?\s*(\d{4})\b",
    re.IGNORECASE
)

def _to_date_iso(y, m, d):
    try:
        return datetime(int(y), int(m), int(d), tzinfo=ZoneInfo("America/Sao_Paulo")).date().isoformat()
    except Exception:
        return None

def parse_date_any(text):
    """FIX: tenta vários formatos e devolve ISO (YYYY-MM-DD) ou None."""
    if not text:
        return None

    # ISO
    m = RE_ISO.search(text)
    if m:
        return _to_date_iso(m.group(1), m.group(2), m.group(3))

    # Numérico dd/mm/aaaa
    m = RE_NUM.search(text)
    if m:
        d, mo, y = m.group(1), m.group(2), m.group(3)
        return _to_date_iso(y, mo, d)

    # d (de) mmm|mês (de) aaaa
    m = RE_MES.search(text.lower())
    if m:
        d, mes_nome, y = m.group(1), m.group(2).lower(), m.group(3)
        mo = MESES.get(mes_nome)
        return _to_date_iso(y, mo, d) if mo else None

    return None

def extract_date_from_article_html(soup):
    """FIX: tenta meta/og/time antes de regex no texto."""
    # meta og/article
    for sel, attr, key in [
        ("meta", {"property": "article:published_time"}, "content"),
        ("meta", {"name": "date"}, "content"),
        ("meta", {"itemprop": "datePublished"}, "content"),
    ]:
        tag = soup.find(sel, attrs=attr)
        if tag and tag.get(key):
            iso = parse_date_any(tag.get(key))
            if iso:
                return iso

    # <time datetime="...">
    t = soup.find("time")
    if t and t.get("datetime"):
        iso = parse_date_any(t.get("datetime"))
        if iso:
            return iso

    # fallback: regex no topo do artigo
    scope = soup.find("article") or soup
    return parse_date_any(scope.get_text(" ", strip=True))

# DEDUP: normalizador de URL (remove tracking/fragmento e padroniza)
def _normalize_link(url: str | None) -> str | None:
    if not url:
        return None
    u = urlsplit(url)
    # remove utm_*, gclid, fbclid e o fragmento
    qs_pairs = [
        (k, v) for (k, v) in parse_qsl(u.query, keep_blank_values=True)
        if not k.lower().startswith("utm_") and k.lower() not in ("gclid", "fbclid")
    ]
    query = "&".join(f"{k}={v}" for k, v in qs_pairs)
    path = u.path.rstrip("/")  # remove barra final
    return urlunsplit((u.scheme.lower(), u.netloc.lower(), path, query, ""))

# DEDUP: normalizador simples de título (fallback quando não há link)
def _normalize_title(t: str | None) -> str:
    if not t:
        return ""
    return re.sub(r"\s+", " ", t).strip().lower()

def coletar_noticias():
    print("Iniciando scraping...")

    todas = []

    # DEDUP: conjunto global (por execução) de itens já vistos
    vistos = set()

    # FIX: “hoje” no fuso correto
    hoje_iso = datetime.now(ZoneInfo("America/Sao_Paulo")).date().isoformat()

    for site in SITES:
        try:
            print(f"Extraindo de {site['fonte']}...")
            r = requests.get(site["url"], headers=HEADERS, timeout=15)
            r.raise_for_status()

            soup = BeautifulSoup(r.text, "html.parser")

            # FIX: REMOVEMOS o gate rígido por data na listagem.
            # Vamos avaliar item a item (tentando bloco e, se preciso, página da matéria).

            for item in soup.select(site["selector"]):
                titulo = item.get_text(strip=True)
                link = item.get("href")

                if link and link.startswith("/"):
                    link = urljoin(site["url"], link)

                # 1) tentar achar data no próprio bloco da listagem
                bloco = item.parent or item
                bloco_txt = bloco.get_text(" ", strip=True) if bloco else titulo
                data_iso = parse_date_any(bloco_txt)

                # 2) se não achar, abrir a matéria e procurar meta/time/regex
                if not data_iso and link:
                    try:
                        r2 = requests.get(link, headers=HEADERS, timeout=15)
                        r2.raise_for_status()
                        soup2 = BeautifulSoup(r2.text, "html.parser")
                        data_iso = extract_date_from_article_html(soup2)
                    except Exception as e_in:
                        print(f"Falha ao abrir matéria ({site['fonte']}): {e_in}")

                # 3) incluir somente se a data for HOJE
                if data_iso == hoje_iso:
                    # DEDUP: gerar chave e pular duplicatas
                    link_norm = _normalize_link(link)
                    chave = link_norm or f"{site['fonte']}|{_normalize_title(titulo)}"
                    if chave in vistos:
                        continue
                    vistos.add(chave)

                    todas.append({
                        "titulo": titulo,
                        "link": link_norm or link,  # envia o link já normalizado, se possível
                        "fonte": site["fonte"],
                        "data": data_iso
                    })

        except Exception as e:
            print(f"Erro ao processar {site['fonte']}: {e}")
            todas.append({
                "titulo": f"Erro ao processar {site['fonte']}",
                "link": None,
                "fonte": str(e),
                "data": None
            })

    print("Total extraído (somente hoje, sem duplicatas):", len(todas))
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
            print("Erro geral na função:", e)
            body = {"erro": str(e)}
            body_str = json.dumps(body, ensure_ascii=False)

            self.send_response(500)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(body_str.encode("utf-8"))

    def do_GET(self):
        self._executar()

    def do_POST(self):
        self._executar()
