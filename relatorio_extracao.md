# Relatório de Extração de Metadados

Este documento resume as estratégias e o status da extração de metadados dos documentos do NatJus.

### Estrutura dos Dados

Os dados são processados a partir de `data/raw_data/NT e PARECERES` e salvos em `data/processed_data`.

### Melhorias na Extração de Metadados (Atualizado)

Implementamos uma extração mais robusta e otimizada:

1.  **Metadados Aprimorados**:
    - **CID**: Suporte para variações de formatação (ex: "E 04.8") e busca em todo o contexto.
    - **Assunto**: Captura inteligente de blocos de texto multilinha até o próximo cabeçalho.
    - **Objeto/Medicamento**: Nova busca por termos chave ("Solicita", "Medicamento") para identificar o pleito.
    - **Desfecho**: Inferência baseada na análise da seção de "Conclusão", categorizando como Favorável/Desfavorável.
    - **Data, Processo e Nota Técnica**: Lógica de fallback para nome do arquivo e regex mais flexíveis.

2.  **Otimização de Performance**:
    - Arquivos PDF muito grandes (>20 páginas) agora têm apenas as primeiras e últimas 10 páginas processadas para extração de texto. Isso evita travamentos em processos gigantes ("CONSULTA INTEGRAL...") sem perder metadados críticos que ficam nas extremidades do documento.

O script de processamento em lote está rodando e aplicando essas novas regras a todo o dataset.
