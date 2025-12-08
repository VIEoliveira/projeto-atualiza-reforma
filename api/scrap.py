from http.server import BaseHTTPRequestHandler
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import json

# Datas/regex/util
from datetime import datetime, timedelta  # ADIÇÃO: timedelta para calcular "últimos N dias"
from zoneinfo import ZoneInfo
import re
import unicodedata

# URL utils p/ dedup
from urllib.parse import urlsplit, urlunsplit, parse_qsl
import hashlib  # ADIÇÃO: para gerar id_unico estável a partir de link/título/fonte

WEBHOOK = "https://hook.us2.make.com/9v46zbanehc2m84vjk1scd4718xwhdmb"

SITES = [
    {
        "url": "https://noticias.iob.com.br/reforma-tributaria/",
        "selector": ".td-module-title a",             # IOB
        "fonte": "IOB Reformas"
    },
    {
        # LegisWeb: resultados em tabela (#busca_res); título em .result-titulo a; data em .result-datado
        "url": "https://www.legisweb.com.br/noticias/?termo=Reforma+Tribut%E1ria&assunto=&acao=Buscar",
        "selector": "#busca_res .result-titulo a",
        "fonte": "LegisWeb"
    },
    {
        # Portal Reforma (Elementor): título em .elementor-post__title a; data em .elementor-post__date
        "url": "https://www.reformatributaria.com/ultimas-noticias/",
        "selector": ".elementor-post__title a",
        "fonte": "Portal Reforma"
    }
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "pt-BR,pt;q=0.9"
}

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

RE_NUM = re.compile(r"\b([0-3]?\d)[/\.]([01]?\d)[/\.](\d{4})\b")   # 02/12/2025, 2/12/2025, 02.12.2025
RE_ISO = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")                # 2025-12-02
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
    # Metas padrão
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

    # <time datetime/text>
    t = soup.find("time")
    if t and t.get("datetime"):
        iso = parse_date_any(t.get("datetime"))
        if iso:
            return iso
    if t and t.get_text(strip=True):
        iso = parse_date_any(t.get_text(strip=True))
        if iso:
            return iso

    # Específicos por site
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

    # Fallback: regex no topo
    scope = soup.find("article") or soup
    return parse_date_any(scope.get_text(" ", strip=True))

# --- FILTRO: APENAS "REFORMA TRIBUTÁRIA" ---

KEYWORDS = [
    # núcleo
    "reforma tributaria", "reforma tributária", "ibs", "cbs", "iva",
    "imposto seletivo", "imposto sobre bens e serviços", "contribuicao sobre bens e servicos",
    "contribuição sobre bens e serviços",
    # regulamentação / leis
    "lei complementar 214", "plp 68/2024", "regulamentação da reforma tributária",
    "emenda constitucional 132",
    # alíquotas e regimes
    "alíquota de referência", "aliquota de referencia", "alíquotas-teste", "aliquotas-teste",
    "estimativa de alíquotas", "regimes favorecidos", "regimes específicos",
    "não-contribuintes", "nao-contribuintes", "isenção", "isencao",
    "redução de 30%", "reducao de 30%", "redução de 60%", "reducao de 60%",
    # governança
    "conselho federativo do ibs", "comitê gestor do ibs", "comite gestor do ibs",
    "administração do ibs e cbs", "arrecadação do ibs", "distribuição do ibs",
    # transições e travas
    "transição para o novo modelo", "transição do ibs e cbs", "transicao",
    "teto da carga tributária", "teto da carga tributaria", "fixação das alíquotas de referência",
    "fixacao das aliquotas de referencia",
    # mecanismos sociais e setoriais
    "cashback de impostos", "cesta básica nacional", "cesta basica nacional",
    "zona franca de manaus", "simples nacional",
    # fundos
    "fundo de desenvolvimento regional", "fundo de compensação de benefícios fiscais",
    "fundo de compensacao de beneficios fiscais",
    # materiais oficiais
    "resumo técnico", "resumo tecnico", "perguntas e respostas",
    "apresentações reforma tributária", "apresentacoes reforma tributaria",
    # siglas usadas em portais
    "rt pro", "rtpro"
]

NEGATIVE = [
    "malha fina", "irpf", "iptu", "itbi", "refis municipal", "spu"
]

def _norm(s: str | None) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", s).strip().lower()

KEY_N = [_norm(k) for k in KEYWORDS]
NEG_N = [_norm(n) for n in NEGATIVE]

def match_reforma_title_url(titulo: str, link: str | None) -> bool:
    t = _norm(titulo)
    u = _norm(link or "")
    pos = any(k in t or k in u for k in KEY_N)
    if not pos:
        return False
    return True

def match_reforma_fulltext(texto: str) -> bool:
    z = _norm(texto)
    pos = any(k in z for k in KEY_N)
    if not pos:
        return False
    return True

# --- DEDUP ---

def _normalize_link(url: str | None) -> str | None:
    """
    ALTERAÇÃO: reforçamos a normalização de URL para bater melhor com o que será usado no Make:
    - removemos parâmetros de tracking (utm_*, gclid, fbclid) [já existia]
    - padronizamos scheme/http/https para 'https'
    - removemos 'www.' do host
    - removemos barra final da path
    - descartamos fragmento (#...)
    """
    if not url:
        return None
    u = urlsplit(url)
    qs_pairs = [
        (k, v) for (k, v) in parse_qsl(u.query, keep_blank_values=True)
        if not k.lower().startswith("utm_") and k.lower() not in ("gclid", "fbclid")
    ]
    query = "&".join(f"{k}={v}" for k, v in qs_pairs)
    path = u.path.rstrip("/")
    # padroniza scheme e host
    scheme = "https" if u.scheme.lower() in ("http", "https") else u.scheme.lower()
    netloc = u.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    return urlunsplit((scheme, netloc, path, query, ""))  # fragment sempre vazio

def _normalize_title(t: str | None) -> str:
    if not t:
        return ""
    return re.sub(r"\s+", " ", t).strip().lower()

def gerar_id_unico(link_norm: str | None, titulo: str | None, fonte: str | None) -> str:
    """
    ADIÇÃO: gera um ID único estável para a notícia.
    - usa link_normalizado (quando existir)
    - + título normalizado
    - + fonte normalizada
    Esse mesmo id_unico pode ser usado como chave no Data Store do Make.
    """
    base_link = (link_norm or "").strip().lower()
    titulo_norm = _normalize_title(titulo)
    fonte_norm = (fonte or "").strip().lower()
    chave_base = f"{base_link}||{titulo_norm}||{fonte_norm}"
    return hashlib.md5(chave_base.encode("utf-8")).hexdigest()

# --- EXTRAÇÃO DO CORPO DA MATÉRIA ---

def extract_article_body_text(soup: BeautifulSoup, fonte: str | None = None, max_chars: int = 8000) -> str:
    """
    ADIÇÃO: tenta extrair o texto principal da matéria.
    Usa seletores específicos por site e, em seguida, fallbacks genéricos.
    Limita o tamanho para não explodir o prompt.
    """
    candidatos = []

    try:
        if fonte == "Portal Reforma":
            seletores = [
                ".elementor-post__text",
                ".elementor-widget-theme-post-content",
                "article .entry-content",
                "article"
            ]
        elif fonte == "LegisWeb":
            seletores = [
                ".noticia-conteudo",
                ".noticia-corpo",
                "article",
                ".conteudo"
            ]
        elif fonte == "IOB Reformas":
            seletores = [
                ".td-post-content",
                "article",
                ".conteudo"
            ]
        else:
            seletores = ["article", "main", "body"]

        for sel in seletores:
            el = soup.select_one(sel)
            if el:
                txt = el.get_text(" ", strip=True)
                if txt and len(txt) > 200:  # evitar pegar blocos muito pequenos irrelevantes
                    candidatos.append(txt)

        if not candidatos:
            # fallback: texto do <body>
            body = soup.find("body")
            if body:
                txt = body.get_text(" ", strip=True)
                if txt and len(txt) > 200:
                    candidatos.append(txt)

        if not candidatos:
            return ""

        texto = max(candidatos, key=len)  # pega o maior bloco de texto encontrado
        if len(texto) > max_chars:
            texto = texto[:max_chars] + " [...]"  # truncamento com indicação
        return texto
    except Exception:
        return ""

# --- DATA NA LISTAGEM (por site) ---

def _extract_list_date_iso(site_fonte: str, item, soup):
    try:
        if site_fonte == "Portal Reforma":
            # cartão do post → span.elementor-post__date
            card = item.find_parent(class_=re.compile(r"elementor-post", re.I)) or item.parent
            el = card.select_one(".elementor-post__date") if card else None
            if el:
                return parse_date_any(el.get_text(" ", strip=True))
        elif site_fonte == "LegisWeb":
            # cada resultado numa <tr>; data em .result-datado
            tr = item.find_parent("tr") or item.parent
            el = tr.select_one(".result-datado") if tr else None
            if el:
                return parse_date_any(el.get_text(" ", strip=True))
        else:
            # IOB: parent costuma ter a meta com a data
            bloco = item.parent or item
            return parse_date_any(bloco.get_text(" ", strip=True))
    except Exception:
        return None
    return None

# --- COLETOR ---

def coletar_noticias():
    print("Iniciando scraping...")

    todas = []
    vistos = set()  # ALTERAÇÃO: agora guarda id_unico, não mais uma chave solta

    # ADIÇÃO: configuração de janela de dias (hoje + ontem = últimos 2 dias)
    hoje = datetime.now(ZoneInfo("America/Sao_Paulo")).date()
    limite = hoje - timedelta(days=1)  # inclui hoje e ontem; ajuste o "1" se quiser mais dias

    for site in SITES:
        try:
            print(f"Extraindo de {site['fonte']}...")
            r = requests.get(site["url"], headers=HEADERS, timeout=15)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

            for item in soup.select(site["selector"]):
                # ------------- TÍTULO (com limpeza específica LegisWeb) -------------
                titulo = item.get_text(" ", strip=True)

                if site["fonte"] == "LegisWeb":
                    # 1) preferir atributo title (é mais limpo)
                    if item.has_attr("title") and item["title"].strip():
                        titulo = item["title"].strip()
                    # 2) remover prefixos comuns da busca
                    titulo = re.sub(r"^\s*reforma\s*tribut[aá]ria\s*[-–—:]\s*", "", titulo, flags=re.IGNORECASE)
                    titulo = re.sub(r"^\s*reformatributaria\s*[-–—:]\s*", "", titulo, flags=re.IGNORECASE)
                    # 3) normalizar espaços
                    titulo = re.sub(r"\s{2,}", " ", titulo).strip()
                # ---------------------------------------------------------------------

                # AJUSTE: sempre normalizar URL (relativa ou absoluta)
                link = item.get("href")
                if link:
                    link = urljoin(site["url"], link)

                # 1) data na LISTAGEM
                data_iso = _extract_list_date_iso(site["fonte"], item, soup)

                # manter soup2 se abrirmos a matéria
                soup2 = None

                # 2) se não achou data, tenta na MATÉRIA
                if not data_iso and link:
                    try:
                        r2 = requests.get(link, headers=HEADERS, timeout=15)
                        r2.raise_for_status()
                        soup2 = BeautifulSoup(r2.text, "html.parser")
                        data_iso = extract_date_from_article_html(soup2, fonte=site["fonte"])
                    except Exception as e_in:
                        print(f"Falha ao abrir matéria ({site['fonte']}): {e_in}")

                # 3) filtro por janela de datas (últimos 2 dias)
                if not data_iso:
                    continue  # sem data não conseguimos garantir recência

                try:
                    data_materia = datetime.fromisoformat(data_iso).date()
                except ValueError:
                    continue  # data inválida/inesperada → descarta

                if data_materia < limite:
                    continue  # muito antiga → descarta

                # 4) filtro de "Reforma Tributária"
                passa = match_reforma_title_url(titulo, link)
                if not passa:
                    # título/URL ambíguo → confirma pelo corpo se já temos, senão abre agora
                    if soup2 is None and link:
                        try:
                            r3 = requests.get(link, headers=HEADERS, timeout=15)
                            r3.raise_for_status()
                            soup2 = BeautifulSoup(r3.text, "html.parser")
                        except Exception:
                            soup2 = None
                    if soup2 is not None:
                        passa = match_reforma_fulltext(soup2.get_text(" ", strip=True))
                if not passa:
                    continue  # não é reforma → descarta

                # 5) garantir que temos HTML da matéria para extrair corpo
                if soup2 is None and link:
                    try:
                        r4 = requests.get(link, headers=HEADERS, timeout=15)
                        r4.raise_for_status()
                        soup2 = BeautifulSoup(r4.text, "html.parser")
                    except Exception as e_html:
                        print(f"Falha ao obter HTML para corpo ({site['fonte']}): {e_html}")
                        soup2 = None

                corpo = ""
                if soup2 is not None:
                    corpo = extract_article_body_text(soup2, fonte=site["fonte"])  # ADIÇÃO: corpo da notícia

                # 6) dedup
                link_norm = _normalize_link(link)
                id_unico = gerar_id_unico(link_norm, titulo, site["fonte"])  # ADIÇÃO: id único usado no Python e no Make
                if id_unico in vistos:
                    continue
                vistos.add(id_unico)

                todas.append({
                    # ADIÇÃO: campos extras para alinhamento com o Data Store do Make
                    "id_unico": id_unico,
                    "link": link,  # agora sempre mandamos link absoluto
                    "link_normalizado": link_norm or link,
                    "titulo": titulo,
                    "fonte": site["fonte"],
                    "data": data_iso,
                    "corpo": corpo  # texto (ou resumo bruto) da matéria
                })

        except Exception as e:
            print(f"Erro ao processar {site['fonte']}: {e}")
            # Sem adicionar notícia de erro em `todas` para não poluir o array

    print("Total extraído (últimos 2 dias, sem duplicatas):", len(todas))
    return todas

# --- HANDLER HTTP ---

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
