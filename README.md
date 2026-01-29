# Teste de Nivelamento - Estágio Intuitive Care 
**Linguagem Utilizada: Python**


Este repositório contém a solução do teste prático para a vaga de estágio na **Intuitive Care**. O objetivo deste projeto foi ir além do funcionamento básico: apliquei conceitos de Engenharia de Dados para criar automação robusta, escalável e documentada, pronta para lidar com cenários reais de variação de dados

## 1. Preparação do Ambiente

Para garantir a reprodutibilidade do teste e manter o sistema global organizado, a solução utiliza um ambiente virtual isolado.

### Pré-requisitos
* **Python 3.10+**
* **Bibliotecas base:** `requests`, `beautifulsoup4`, `pandas`

### Configuração Passo a Passo
1. **Criação do Ambiente Virtual (venv):**
   Utilizei o terminal integrado do VS Code para criar um ambiente isolado:
   ```bash
   python -m venv venv
### Ativação do Ambiente

* **Windows:**
  ```bash
  .\venv\Scripts\activate

* **macOS/Linux:**
```bash
source venv/bin/activate
```

---

## 2. Etapa 1.1: Integração com API Pública da ANS

O objetivo desta etapa é acessar a API de Dados Abertos da ANS e realizar o download das **Demonstrações Contábeis** dos últimos 3 trimestres disponíveis.

### Execução do Script
O script de coleta está localizado na pasta `backend`:
```bash
python backend/stage_1_api.py
```
### Decisões Técnicas e Trade-offs

* **Mapeamento Dinâmico de URLs:**
    * **Decisão:** O código utiliza a biblioteca `BeautifulSoup` para navegar nas tags HTML do servidor da ANS, identificando pastas no formato `YYYY/QQ/`.
    * **Justificativa:** A estrutura de diretórios pode mudar ao longo do tempo. Esta abordagem torna o script adaptável a variações de nomenclatura e garante a captura automática dos dados mais recentes sem intervenção manual.

* **Persistência Incremental (Staging Area):**
    * **Decisão:** Armazenar os arquivos ZIP originais em uma pasta local (`data/raw`) antes de iniciar o processamento.
    * **Justificativa:** Dado o volume de dados e a instabilidade potencial de APIs públicas, ter os dados brutos salvos permite repetir etapas de extração e limpeza sem a necessidade de novos downloads, economizando tempo e banda de rede.

* **Qualidade de Código (PEP 8):**
    * **Decisão:** Refatoração para manter limites de caracteres e espaçamento padrão.
    * **Justificativa:** Focar na legibilidade e manutenibilidade do código, facilitando a revisão técnica.

### Resultados da Execução (Etapa 1.1)

A execução do script `stage_1_api.py` realizou a varredura recursiva no servidor da ANS e identificou os trimestres mais recentes.

**1. Log de Execução:**
![Log do Terminal com Sucesso](assets/image1.png)
*Figura 1: Terminal exibindo a identificação dos anos e o download dos arquivos ZIP.*

**2. Persistência dos Dados:**
![Arquivos Baixados](assets/image2.png)
*Figura 2: Verificação da pasta `data/raw` contendo os arquivos `1T2025.zip`, `2T2025.zip` e `3T2025.zip`.*

## Etapa 1.2: Pipeline de Transformação e Limpeza (ETL)

Esta etapa é responsável por processar os arquivos brutos (ZIPs), normalizar as discrepâncias e consolidar os dados para análise.

### 1. Decisão de Arquitetura: Processamento Incremental
Para garantir performance e estabilidade, optei por uma abordagem de **Batch Processing Incremental** ao invés de carregar todos os dados em memória (*In-Memory*).

* **Implementação:** O script processa um arquivo ZIP por vez (extração -> transformação -> carga -> limpeza temporária).
* **Justificativa (Trade-off):** Optei por processar os dados aos poucos (incrementalmente) em vez de carregar tudo de uma vez, garantimos a estabilidade do sistema. Essa abordagem impede que a memória acabe (erro de memória cheia), permitindo que o script processe volumes gigantescos de dados sem falhar, mesmo em máquinas com pouca potência.
### 2. Estratégia de Normalização (Data Wrangling)
Para atender ao desafio de **variedade de formatos** (CSV, TXT, colunas inconsistentes) e **evolução de schema**, implementei uma camada de adaptação semântica:

* **Ingestão Polimórfica:** O pipeline detecta automaticamente a extensão e aplica estratégias de *fallback* para diferentes encodings (`latin1` vs `utf-8`) e separadores (`;` vs `,`), garantindo a leitura correta tanto de arquivos legados quanto modernos.
* **Mapeamento Canônico (`Schema Mapping`):** Utilização de um dicionário de tradução para unificar nomenclaturas variadas da ANS.
    * *Exemplo:* As colunas `DT_REGISTRO`, `DATA` e `ANO_TRIMESTRE` são todas normalizadas para o campo único `data_referencia`.
    * **Resiliência:** Colunas essenciais ausentes nos arquivos mais antigos são geradas com valores nulos (`None`), mantendo a integridade da estrutura final.
* **Sanitização de Tipos:** Conversão robusta de valores monetários no formato brasileiro (ex: `"1.000,00"`) para floats computáveis (`1000.0`).

### 3. Resultados da Execução
O pipeline foi capaz de processar e unificar os dados dos 3 trimestres com sucesso.

* **Volume Processado:** **2.113.924 registros** consolidados.
* **Arquivo Final:** `data/processed/despesas_consolidadas.csv`

**Evidência de Performance:**
![Sucesso no ETL](assets/image4.png)
*Figura 3: Log de execução comprovando o processamento de mais de 2 milhões de linhas com a estratégia incremental.*

**Amostra dos Dados Consolidados:**
![Preview do Arquivo CSV](assets/image5.png)
*Figura 4: Visualização da estrutura do arquivo `despesas_consolidadas.csv`, demonstrando a unificação das colunas e a identificação da origem dos dados.*