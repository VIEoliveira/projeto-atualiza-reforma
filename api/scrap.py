from http.server import BaseHTTPRequestHandler
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import json

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import re
import unicodedata

from urllib.parse import urlsplit, urlunsplit, parse_qsl
import hashlib

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# CONFIG (Vercel Env Vars)
# =========================
# Defina no Vercel:
# - N8N_WEBHOOK_URL = URL do Webhook (PRODUCTION) do n8n
# - DAYS_WINDOW = 1 (hoje+ontem) ou 2 (hoje + 2 dias anteriores), etc.
WEBHOOK = os.getenv("N8N_WEBHOOK_URL", "").strip()
DAYS_WINDOW = int(os.getenv("DAYS_WINDOW", "1"))  # 1 = hoje e ontem

SITES = [
    {"url": "https://noticias.iob.com.br/reforma-tributaria/", "selector": ".td-module-title a", "fonte": "IOB Reformas"},
    {"url": "https://www.legisweb.com.br/noticias/?termo=Reforma+Tribut%E1ria&assunto=&acao=Buscar", "selector": "#busca_res .result-titulo a", "fonte": "LegisWeb"},
    {"url": "https://www.reformatributaria.com/ultimas-noticias/", "selector": ".elementor-post__title a", "fonte": "Portal Reforma"},
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "pt-BR,pt;q=0.9",
}

# =========================
# SESSION + RETRY
# =========================
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(
        total=4,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

SESSION = make_session()

# --- PARSE DE DATAS ---
MESES = {
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

RE_NUM = re.compile(r"\b([0-3]?\d)[/\.]([01]?\d)[/\.](\d{4})\b")
RE_ISO = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")
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
    if not text:
        return None
    m = RE_ISO.search(text)
    if m:
        return _to_date_iso(m.group(1), m.group(2), m.group(3))
    m = RE_NUM.search(text)
    if m:
        d, mo, y = m.group(1), m.group(2), m.group(3)
        return _to_date_iso(y, mo, d)
    m = RE_MES.search(text.lower())
    if m:
        d, mes_nome, y = m.group(1), m.group(2).lower(), m.group(3)
        mo = MESES.get(mes_nome)
        return _to_date_iso(y, mo, d) if mo else None
    return None

# --- EXTRAÇÃO DE DATA NA MATÉRIA ---
def extract_date_from_article_html(soup, fonte=None):
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

    t = soup.find("time")
    if t and t.get("datetime"):
        iso = parse_date_any(t.get("datetime"))
        if iso:
            return iso
    if t and t.get_text(strip=True):
        iso = parse_date_any(t.get_text(strip=True))
        if iso:
            return iso

    if fonte == "Portal Reforma":
        for sel in [".elementor-post-date", ".td-post-date", ".tdb-meta-date", ".entry-date"]:
            el = soup.select_one(sel)
            if el:
                iso = parse_date_any(el.get_text(" ", strip=True))
                if iso:
                    return iso

    if fonte == "LegisWeb":
        for sel in [".data", ".data-publicacao", ".noticia-data", ".dt-publicacao"]:
            el = soup.select_one(sel)
            if el:
                iso = parse_date_any(el.get_text(" ", strip=True))
                if iso:
                    return iso

    scope = soup.find("article") or soup
    return parse_date_any(scope.get_text(" ", strip=True))

# --- FILTRO ---
KEYWORDS = [
    "reforma tributaria", "reforma tributária", "ibs", "cbs", "iva",
    "imposto seletivo", "imposto sobre bens e serviços", "contribuicao sobre bens e servicos",
    "contribuição sobre bens e serviços",
    "lei complementar 214", "plp 68/2024", "regulamentação da reforma tributária",
    "emenda constitucional 132",
    "alíquota de referência", "aliquota de referencia",
    "cashback de impostos", "cesta básica nacional", "simples nacional",
    "conselho federativo do ibs", "comitê gestor do ibs", "comite gestor do ibs",
    "zona franca de manaus",
    "rt pro", "rtpro",
]

NEGATIVE = ["malha fina", "irpf", "iptu", "itbi", "refis municipal", "spu"]

def _norm(s: str | None) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", s).strip().lower()

KEY_N = [_norm(k) for k in KEYWORDS]
NEG_N = [_norm(n) for n in NEGATIVE]

def _has_negative(z: str) -> bool:
    return any(n in z for n in NEG_N)

def match_reforma_title_url(titulo: str, link: str | None) -> bool:
    t = _norm(titulo)
    u = _norm(link or "")
    if _has_negative(t) or _has_negative(u):
        return False
    return any(k in t or k in u for k in KEY_N)

def match_reforma_fulltext(texto: str) -> bool:
    z = _norm(texto)
    if _has_negative(z):
        return False
    return any(k in z for k in KEY_N)

# --- URL / DEDUP ---
def _normalize_link(url: str | None) -> str | None:
    if not url:
        return None
    u = urlsplit(url)
    qs_pairs = [
        (k, v) for (k, v) in parse_qsl(u.query, keep_blank_values=True)
        if not k.lower().startswith("utm_")
        and k.lower() not in ("gclid", "fbclid", "amp", "ref")
    ]
    query = "&".join(f"{k}={v}" for k, v in qs_pairs)
    path = u.path.rstrip("/")
    scheme = "https" if u.scheme.lower() in ("http", "https") else u.scheme.lower()
    netloc = u.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    return urlunsplit((scheme, netloc, path, query, ""))

def _normalize_title(t: str | None) -> str:
    if not t:
        return ""
    return re.sub(r"\s+", " ", t).strip().lower()

def gerar_id_unico(link_norm: str | None, titulo: str | None, fonte: str | None) -> str:
    base_link = (link_norm or "").strip().lower()
    titulo_norm = _normalize_title(titulo)
    fonte_norm = (fonte or "").strip().lower()
    chave_base = f"{base_link}||{titulo_norm}||{fonte_norm}"
    return hashlib.sha256(chave_base.encode("utf-8")).hexdigest()

def extract_canonical_url(soup: BeautifulSoup, base_url: str | None) -> str | None:
    """Prioriza canonical/og:url quando houver."""
    try:
        canon = soup.select_one('link[rel="canonical"]')
        if canon and canon.get("href"):
            return urljoin(base_url or "", canon.get("href"))
        og = soup.find("meta", attrs={"property": "og:url"})
        if og and og.get("content"):
            return urljoin(base_url or "", og.get("content"))
    except Exception:
        pass
    return None

DOC_HINT_RE = re.compile(r"(nota[\s\-]?tecnica|manual|layout|schema|xml|nfe|nf-e|nfce|nfc-e|cte|ct-e|mdfe|mdf-e)", re.I)
DOC_EXT_RE = re.compile(r"\.(pdf|xml|zip|rar|docx?)($|\?)", re.I)

def extract_doc_links(soup: BeautifulSoup, page_url: str) -> list[str]:
    links = []
    try:
        for a in soup.select("a[href]"):
            href = a.get("href")
            if not href:
                continue
            full = urljoin(page_url, href)
            txt = (a.get_text(" ", strip=True) or "")
            if DOC_EXT_RE.search(full) or DOC_HINT_RE.search(full) or DOC_HINT_RE.search(txt):
                links.append(full)
    except Exception:
        return []
    # dedupe preservando ordem
    seen = set()
    out = []
    for u in links:
        nu = _normalize_link(u) or u
        if nu not in seen:
            seen.add(nu)
            out.append(u)
    return out[:12]

# --- EXTRAÇÃO DO CORPO ---
def extract_article_body_text(soup: BeautifulSoup, fonte: str | None = None, max_chars: int = 8000) -> str:
    candidatos = []
    try:
        if fonte == "Portal Reforma":
            seletores = [".elementor-post__text", ".elementor-widget-theme-post-content", "article .entry-content", "article"]
        elif fonte == "LegisWeb":
            seletores = [".noticia-conteudo", ".noticia-corpo", "article", ".conteudo"]
        elif fonte == "IOB Reformas":
            seletores = [".td-post-content", "article", ".conteudo"]
        else:
            seletores = ["article", "main", "body"]

        for sel in seletores:
            el = soup.select_one(sel)
            if el:
                txt = el.get_text(" ", strip=True)
                if txt and len(txt) > 200:
                    candidatos.append(txt)

        if not candidatos:
            body = soup.find("body")
            if body:
                txt = body.get_text(" ", strip=True)
                if txt and len(txt) > 200:
                    candidatos.append(txt)

        if not candidatos:
            return ""

        texto = max(candidatos, key=len)
        if len(texto) > max_chars:
            texto = texto[:max_chars] + " [...]"
        return texto
    except Exception:
        return ""

# --- DATA NA LISTAGEM ---
def _extract_list_date_iso(site_fonte: str, item, soup):
    try:
        if site_fonte == "Portal Reforma":
            card = item.find_parent(class_=re.compile(r"elementor-post", re.I)) or item.parent
            el = card.select_one(".elementor-post__date") if card else None
            if el:
                return parse_date_any(el.get_text(" ", strip=True))
        elif site_fonte == "LegisWeb":
            tr = item.find_parent("tr") or item.parent
            el = tr.select_one(".result-datado") if tr else None
            if el:
                return parse_date_any(el.get_text(" ", strip=True))
        else:
            bloco = item.parent or item
            return parse_date_any(bloco.get_text(" ", strip=True))
    except Exception:
        return None
    return None

# --- COLETOR ---
def coletar_noticias():
    todas = []
    vistos = set()

    hoje = datetime.now(ZoneInfo("America/Sao_Paulo")).date()
    limite = hoje - timedelta(days=DAYS_WINDOW)

    for site in SITES:
        try:
            r = SESSION.get(site["url"], timeout=20)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

            for item in soup.select(site["selector"]):
                titulo = item.get_text(" ", strip=True)

                if site["fonte"] == "LegisWeb":
                    if item.has_attr("title") and item["title"].strip():
                        titulo = item["title"].strip()
                    titulo = re.sub(r"^\s*reforma\s*tribut[aá]ria\s*[-–—:]\s*", "", titulo, flags=re.IGNORECASE)
                    titulo = re.sub(r"^\s*reformatributaria\s*[-–—:]\s*", "", titulo, flags=re.IGNORECASE)
                    titulo = re.sub(r"\s{2,}", " ", titulo).strip()

                link = item.get("href")
                if link:
                    link = urljoin(site["url"], link)

                data_iso = _extract_list_date_iso(site["fonte"], item, soup)
                soup2 = None

                if not data_iso and link:
                    try:
                        r2 = SESSION.get(link, timeout=20)
                        r2.raise_for_status()
                        soup2 = BeautifulSoup(r2.text, "html.parser")
                        data_iso = extract_date_from_article_html(soup2, fonte=site["fonte"])
                    except Exception:
                        soup2 = None

                if not data_iso:
                    continue

                try:
                    data_materia = datetime.fromisoformat(data_iso).date()
                except ValueError:
                    continue

                if data_materia < limite:
                    continue

                passa = match_reforma_title_url(titulo, link)
                if not passa:
                    if soup2 is None and link:
                        try:
                            r3 = SESSION.get(link, timeout=20)
                            r3.raise_for_status()
                            soup2 = BeautifulSoup(r3.text, "html.parser")
                        except Exception:
                            soup2 = None
                    if soup2 is not None:
                        passa = match_reforma_fulltext(soup2.get_text(" ", strip=True))
                if not passa:
                    continue

                if soup2 is None and link:
                    try:
                        r4 = SESSION.get(link, timeout=20)
                        r4.raise_for_status()
                        soup2 = BeautifulSoup(r4.text, "html.parser")
                    except Exception:
                        soup2 = None

                corpo = ""
                doc_candidatos = []
                link_final = link

                if soup2 is not None and link:
                    # Canonical melhora dedupe e link “bonito”
                    canon = extract_canonical_url(soup2, base_url=link)
                    if canon:
                        link_final = canon

                    corpo = extract_article_body_text(soup2, fonte=site["fonte"])
                    doc_candidatos = extract_doc_links(soup2, page_url=link_final or link)

                link_norm = _normalize_link(link_final)
                id_unico = gerar_id_unico(link_norm, titulo, site["fonte"])
                if id_unico in vistos:
                    continue
                vistos.add(id_unico)

                todas.append({
                    "id_unico": id_unico,
                    "link": link_final or link,
                    "link_normalizado": link_norm or (link_final or link),
                    "titulo": titulo,
                    "fonte": site["fonte"],
                    "data": data_iso,
                    "corpo": corpo,
                    "doc_candidatos": doc_candidatos,
                })

        except Exception as e:
            print(f"Erro ao processar {site['fonte']}: {e}")

    return todas

# --- HANDLER ---
class handler(BaseHTTPRequestHandler):
    def _executar(self):
        try:
            if not WEBHOOK:
                raise RuntimeError("N8N_WEBHOOK_URL não definido nas variáveis de ambiente.")

            noticias = coletar_noticias()

            # Envia para n8n (Webhook Trigger)
            try:
                resp = SESSION.post(WEBHOOK, json={"noticias": noticias}, timeout=25)
                print("Resposta do n8n:", resp.status_code, resp.text[:200])
            except Exception as e:
                print("Erro ao chamar o webhook do n8n:", e)

            body = {"mensagem": "Scraping enviado com sucesso ao n8n!", "quantidade_noticias": len(noticias)}
            body_str = json.dumps(body, ensure_ascii=False)

            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(body_str.encode("utf-8"))

        except Exception as e:
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
