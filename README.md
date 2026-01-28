# Teste de Nivelamento - Estágio Intuitive Care 
**Linguagem Utilizada: Python**


Este repositório contém a solução do teste prático para a vaga de estágio na **Intuitive Care**. O projeto foi desenvolvido para demonstrar a capacidade de resolver problemas práticos, tomar decisões técnicas fundamentadas e documentar processos de engenharia de dados.

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
    * **Justificativa:** A estrutura de diretórios pode mudar ao longo do tempo. Esta abordagem torna o script resiliente a variações de nomenclatura e garante a captura automática dos dados mais recentes sem intervenção manual.

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