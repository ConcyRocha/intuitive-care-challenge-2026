import os
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin


BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"
OUTPUT_DIR = os.path.join("data", "raw")
MAX_RECURSION_DEPTH = 2


def get_soup(url):
    """
    Realiza uma requisição HTTP GET e retorna o objeto BeautifulSoup parseado.

    Gerencia exceções de conexão e timeout para evitar quebra do pipeline.

    Args:
        url (str): A URL alvo para raspagem.

    Returns:
        BeautifulSoup: Objeto pronto para extração de tags, ou None se 
        houver erro.
    """
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return BeautifulSoup(response.text, 'html.parser')
    except requests.RequestException as e:
        print(f"   [Erro Conexão] {url}: {e}")
        return None


def parse_quarter_from_filename(filename):
    """
    Extrai metadados temporais (Ano e Trimestre) do nome do arquivo usando Regex.

    Suporta múltiplos padrões de nomenclatura da ANS:
    - Conciso: '1T2025.zip'
    - Extenso: '2008_1_trimestre.zip'
    - Curto: '3t_24.zip'

    Args:
        filename (str): Nome do arquivo (ex: '1T2025.zip').

    Returns:
        tuple: Uma tupla (ano: int, trimestre: int) ou None se não 
        casar padrão.
    """
    filename = filename.lower()  
    match_modern = re.search(r'(\d)t(\d{2,4})', filename)
    if match_modern:
        quarter = int(match_modern.group(1))
        year_raw = match_modern.group(2)
        year = int(year_raw) if len(year_raw) == 4 else int(f"20{year_raw}")
        return year, quarter

    match_verbose = re.search(r'(\d{4})_(\d)_trim', filename)
    if match_verbose:
        year = int(match_verbose.group(1))
        quarter = int(match_verbose.group(2))
        return year, quarter

    return None


def crawl_for_zips(url, current_depth=0):
    """
    Varre recursivamente a URL e subdiretórios em busca de arquivos .zip.

    Utiliza recursão controlada (MAX_RECURSION_DEPTH) para navegar na estrutura
    de pastas do servidor FTP/HTTP da ANS sem entrar em loops infinitos.

    Args:
        url (str): URL atual para inspeção.
        current_depth (str): Profundidade atual da recursão (padrão 0).

    Returns:
        list: Lista de dicionários contendo metadados {'year', 'quarter', 
        'filename', 'url'}.
    """
    found_files = [] 
    if current_depth > MAX_RECURSION_DEPTH:
        return found_files

    soup = get_soup(url)
    if not soup:
        return found_files

    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']     
        if href in ['../', './'] or href.startswith('?') or href.startswith('/'):
            continue

        full_url = urljoin(url, href)
        filename = href.strip("/")

        if href.lower().endswith('.zip'):
            meta = parse_quarter_from_filename(filename)
            if meta:
                year, quarter = meta
                found_files.append({
                    "year": year,
                    "quarter": quarter,
                    "filename": filename,
                    "url": full_url
                })
        
        elif href.endswith('/'):
            print(f"   [Recursão Nível {current_depth+1}]"
                  f" Entrando na pasta: {filename}")
            subfolder_files = crawl_for_zips(full_url, current_depth + 1)
            found_files.extend(subfolder_files)

    return found_files


def main():
    """
    Pipeline de Extração (Etapa 1.1).

    Fluxo de Execução:
    1. Acessa a raiz do repositório da ANS.
    2. Identifica pastas de Anos (ex: '2025/', '2024/').
    3. Varre os 3 anos mais recentes buscando ZIPs recursivamente.
    4. Seleciona apenas os 3 trimestres cronologicamente mais novos.
    5. Realiza o download dos arquivos selecionados para 'data/raw'.
    """
    print(">>> Iniciando Etapa 1.1: Coleta Resiliente e Recursiva")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"Acessando Raiz: {BASE_URL}...")
    soup = get_soup(BASE_URL)  
    if not soup:
        print("Erro crítico: Não foi possível acessar a URL base.")
        return

    years_found = []
    for a in soup.find_all('a', href=True):
        clean_href = a['href'].strip("/")
        if re.match(r"^\d{4}$", clean_href):
            full_url = urljoin(BASE_URL, a['href'])
            years_found.append((int(clean_href), full_url)) 
    years_found.sort(key=lambda x: x[0], reverse=True)

    if not years_found:
        print("Nenhum ano encontrado.")
        return

    all_candidates = []
    print(f"Varrendo os anos mais recentes: {[y[0] for y in years_found[:3]]}") 
    for year_val, year_url in years_found[:3]:
        print(f"\nVerificando ano: {year_val}...")
        files_in_year = crawl_for_zips(year_url)
        all_candidates.extend(files_in_year)

    all_candidates.sort(key=lambda x: (x['year'], x['quarter']))
    last_3_quarters = all_candidates[-3:]

    if not last_3_quarters:
        print("Nenhum arquivo de trimestre válido encontrado.")
        return

    print(f"\n=== Resultado Final: Últimos 3 Trimestres Encontrados ===")
    for item in last_3_quarters:
        print(f" -> {item['year']} / {item['quarter']}º Tri: {item['filename']}")

    print("\n>>> Iniciando Downloads...")
    for item in last_3_quarters:
        filepath = os.path.join(OUTPUT_DIR, item['filename']) 
        if os.path.exists(filepath):
            print(f"   [Pular] {item['filename']} já existe.")
            continue

        print(f"   [Baixando] {item['filename']}...")
        try:
            with requests.get(item['url'], stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(filepath, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            print("   Sucesso!")
        except Exception as e:
            print(f"   [Falha] Erro ao baixar {item['filename']}: {e}")


if __name__ == "__main__":
    main()