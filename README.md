
# Indexação de Notas Tecnicas Legado (NatJus) - DOCS

Esta documentacao tem como objetivo processar metadados  extraídos de Notas Técnicas e Pareceres legado, higienizar os dados e indexá-los no Elasticsearch para consulta.

## Pré-requisitos e Ambiente

Antes de executar os scripts, certifique-se de que o ambiente possui:



3. **Acesso ao Elasticsearch** (URL, usuário e senha).
4. **Variáveis de Ambiente** configuradas (crie um arquivo `.env` ou exporte no terminal):

```bash
# Exemplo de configuração
export ELASTICSEARCH_URL="http://seu-servidor-elastic:9200"
export ELASTIC_USERNAME="elastic"
export ELASTIC_PASSWORD="sua_senha"

```

---

## Estrutura de Arquivos Necessária

Para que a execução funcione corretamente, organize o diretório do projeto da seguinte forma:

```text
projeto-natjus/
│
├── 01_pre_process.py        # Script de limpeza e padronização (antigo pre_process_fix.py)
├── 02_index_legacy.py       # Script de indexação no Elastic (antigo index_legacy_json.py)
│
└── data/                    # PASTA OBRIGATÓRIA
    └── processed_data/      # PASTA OBRIGATÓRIA
        ├── metadados_extraidos.csv   # Arquivo de entrada (pode estar em Latin1 ou UTF-8)
        └── metadados_extraidos.json  # Arquivo de entrada (JSON bruto)

```

---

## Passo a Passo de Execução

Siga a ordem abaixo para garantir a integridade dos dados.

### 1. Clonar o Projeto

Baixe o repositório para sua máquina local.

```bash
git clone https://github.com/lucalazengo/natjus_extract
cd projeto-natjus

```

### 2. Adicionar a Pasta de Dados

Como dados processados geralmente não são versionados (estão no `.gitignore`), você deve criar a estrutura manualmente e adicionar os artefatos.

1. Crie a pasta `data` na raiz.
2. Dentro dela, crie a pasta `processed_data`.

### 3. Adicionar os Artefatos

Copie os arquivos gerados para a pasta criada:

* Copie `metadados_extraidos.csv` para `data/processed_data/`
* Copie `metadados_extraidos.json` para `data/processed_data/`

### 4. Executar `01_pre_process.py`

Este script prepara os dados para o novo padrão do sistema.

```bash
python 01_pre_process.py

```

**O que este script faz:**

* **Corrige Codificação:** Lê o CSV detectando automaticamente se está em `UTF-8`, `Latin1` ou `UTF-16` (comum se veio do Excel).
* **Limpa Sujeira:** Remove colunas "fantasmas" ou dados excedentes que quebram a estrutura tabular.
* **Adiciona Campo `tipo`:** Insere a coluna/chave `tipo` com o valor **"legado"** em todos os registros.
* **Padroniza Saída:** Salva o CSV corrigido em `UTF-8-SIG` e atualiza o JSON.

### 5. Executar `02_index_legacy.py`

Este script envia os dados tratados para o banco de dados de busca.

```bash
python 02_index_legacy.py

```

**O que este script faz:**

* **Leitura Dinâmica:** Localiza o JSON na pasta `data/processed_data` 
* **Conversão de Datas:** Transforma datas textuais (ex: "10 de Janeiro de 2024") para o formato ISO (`2024-01-10`) exigido pelo Elasticsearch.
* **Bulk Insert:** Envia os dados para o Elasticsearch em lotes de 1.000 documentos 
* **Indexação:** Grava no índice `vw-natjus`, garantindo que o campo `tipo` seja indexado como "legado".

---
