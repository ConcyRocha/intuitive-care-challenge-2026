import os
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin


BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"
OUTPUT_DIR = os.path.join("data", "raw")
MAX_RECURSION_DEPTH = 2


def get_soup(url):
    """Retorna o objeto BeautifulSoup de uma URL, tratando erros."""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return BeautifulSoup(response.text, 'html.parser')
    except requests.RequestException as e:
        print(f"   [Erro Conexão] {url}: {e}")
        return None


def parse_quarter_from_filename(filename):
    """
    Extrai Ano e Trimestre do nome do arquivo usando Regex.
    Suporta: '1T2025.zip', '2008_1_trimestre.zip', '3t_24.zip'
    """
    filename = filename.lower()  
    # Padrão atual: 1t2025, 3t24
    match_modern = re.search(r'(\d)t(\d{2,4})', filename)
    if match_modern:
        quarter = int(match_modern.group(1))
        year_raw = match_modern.group(2)
        year = int(year_raw) if len(year_raw) == 4 else int(f"20{year_raw}")
        return year, quarter

    # Padrão Antigo/Verbos: 2008_1_trimestre
    match_verbose = re.search(r'(\d{4})_(\d)_trim', filename)
    if match_verbose:
        year = int(match_verbose.group(1))
        quarter = int(match_verbose.group(2))
        return year, quarter

    return None


def crawl_for_zips(url, current_depth=0):
    """
    Função RECURSIVA que varre a URL e suas subpastas em busca de ZIPs.
    """
    found_files = [] 
    # Para de cavar se atingir o limite de profundidade
    if current_depth > MAX_RECURSION_DEPTH:
        return found_files

    soup = get_soup(url)
    if not soup:
        return found_files

    # Itera sobre todos os links da página
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']     
        # Ignora links de navegação do servidor (Parent Directory, Ordenação)
        if href in ['../', './'] or href.startswith('?') or href.startswith('/'):
            continue

        full_url = urljoin(url, href)
        filename = href.strip("/")

        # CASO 1: É um arquivo ZIP?
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
        
        # CASO 2: É uma subpasta
        elif href.endswith('/'):
            print(f"   [Recursão Nível {current_depth+1}]"
                  f" Entrando na pasta: {filename}")
            subfolder_files = crawl_for_zips(full_url, current_depth + 1)
            found_files.extend(subfolder_files)

    return found_files


def main():
    print(">>> Iniciando Etapa 1.1: Coleta Resiliente e Recursiva")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Identificar Anos Disponíveis 
    print(f"Acessando Raiz: {BASE_URL}...")
    soup = get_soup(BASE_URL)  
    if not soup:
        print("Erro crítico: Não foi possível acessar a URL base.")
        return

    # Lista preliminar de anos
    years_found = []
    for a in soup.find_all('a', href=True):
        clean_href = a['href'].strip("/")
        if re.match(r"^\d{4}$", clean_href):
            full_url = urljoin(BASE_URL, a['href'])
            years_found.append((int(clean_href), full_url)) 
    # Ordena decrescente (mais recentes primeiro)
    years_found.sort(key=lambda x: x[0], reverse=True)

    if not years_found:
        print("Nenhum ano encontrado.")
        return

    all_candidates = []
    # Top 3 anos para garantir os 3 últimos tri
    print(f"Varrendo os anos mais recentes: {[y[0] for y in years_found[:3]]}") 
    for year_val, year_url in years_found[:3]:
        print(f"\nVerificando ano: {year_val}...")
        # Chama a função que lida com pastas 
        files_in_year = crawl_for_zips(year_url)
        all_candidates.extend(files_in_year)

    # 3. Filtrar os top 3 cronológicos
    # Ordena: Ano (Crescente) -> Trimestre (Crescente)
    all_candidates.sort(key=lambda x: (x['year'], x['quarter']))
    last_3_quarters = all_candidates[-3:]

    if not last_3_quarters:
        print("Nenhum arquivo de trimestre válido encontrado.")
        return

    print(f"\n=== Resultado Final: Últimos 3 Trimestres Encontrados ===")
    for item in last_3_quarters:
        print(f" -> {item['year']} / {item['quarter']}º Tri: {item['filename']}")

    # 4. Download
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