from pypdf import PdfReader
from io import BytesIO
import os
from elasticsearch import Elasticsearch
from docx import Document as DocxDocument

from models.parecer_cid import ParecerCID
from models.parecer_objeto import ParecerObjeto
from models.parecer_classificador import ParecerClassificador
from models.parecer_insumo import ParecerInsumo
from models.parecer_medicamento import ParecerMedicamento

NOME_INDICE = "vw-natjus-tjgo"

cliente = Elasticsearch(
    os.getenv("ELASTICSEARCH_URL"),
    basic_auth=(os.getenv("ELASTIC_USERNAME"), os.getenv("ELASTIC_PASSWORD")),
    verify_certs=False,
    ssl_show_warn=False,
    request_timeout=60,
    retry_on_timeout=True
)

TEM_DOCX = True


def create_index_with_new_mapping(client, index_name):
    """
    ATENÇÃO: deletar e recriar remove todos os documentos do índice.
    Use apenas quando você quiser "resetar" o índice (ex.: em ambiente de teste).
    """
    if client.indices.exists(index=index_name):
        print(f"Índice '{index_name}' já existe. Deletando para recriar com o novo mapeamento...")
        client.indices.delete(index=index_name)

    print(f"Criando índice '{index_name}' com o novo mapeamento...")

    mapping = {
        "properties": {

            "source_filename": {"type": "keyword"},
            "tipo_arquivo": {"type": "keyword"},
            "processo": {"type": "keyword"},
            "cid": {"type": "keyword"},
            "n_nota_tecnica": {"type": "text", "analyzer": "portuguese"},
            "desfecho": {"type": "keyword"},
            "inteiro_teor": {"type": "text", "analyzer": "portuguese"},

            "objeto": {"type": "text", "analyzer": "portuguese"},
            "classificador_do_objeto": {"type": "text", "analyzer": "portuguese"},
            "informacao_complementar": {"type": "text", "analyzer": "portuguese"},
            "medicamento_e_insumo": {"type": "text", "analyzer": "portuguese"},
            "data_do_envio": {"type": "date", "format": "yyyy-MM-dd"},
        }
    }

    client.indices.create(index=index_name, mappings=mapping)
    print("Novo índice criado com sucesso.")


def garantir_indice(client, index_name):
    """
    Se RECRIAR_INDICE_ELASTIC=1/true, recria o índice com o mapping acima.
    Caso contrário, cria apenas se não existir.
    """
    recriar = (os.getenv("RECRIAR_INDICE_ELASTIC", "").strip().lower() in {"1", "true", "sim", "yes"})
    existe = client.indices.exists(index=index_name)

    if recriar:
        create_index_with_new_mapping(client, index_name)
        return

    if not existe:
        print(f"Índice '{index_name}' não existe. Criando com o novo mapeamento...")
        create_index_with_new_mapping(client, index_name)


def extrair_texto(nome_arquivo, arquivo_bytes):
    _, ext = os.path.splitext(nome_arquivo)
    ext = ext.lower()
    texto = ""

    try:
        if ext == ".pdf":
            reader = PdfReader(BytesIO(arquivo_bytes))
            for page in reader.pages:
                texto += (page.extract_text() or "") + "\n"

        elif ext == ".docx" and TEM_DOCX:
            doc = DocxDocument(BytesIO(arquivo_bytes))
            for para in doc.paragraphs:
                texto += para.text + "\n"

        elif ext in [".doc", ".odt", ".txt"]:
            texto = arquivo_bytes.decode("utf-8", errors="ignore")

        else:
            print(f"Formato de arquivo não suportado para extração de texto: {ext}")
            return None

    except Exception as e:
        print(f"Erro ao ler {nome_arquivo}: {e}")
        return None

    return texto.strip()


def criar_documento(nome_arquivo, arquivo_bytes, parecer_model):

    garantir_indice(cliente, NOME_INDICE)

    texto = extrair_texto(nome_arquivo, arquivo_bytes)
    if not texto:
        print(f"Nenhum texto extraído do arquivo: {nome_arquivo}")
        return None

    processo = getattr(parecer_model, "n_processo", None)
    nota_tecnica = getattr(parecer_model, "nota_tecnica", None)
    data_do_envio = getattr(parecer_model, "dt_envio", None)
    informacao_complementar = getattr(parecer_model, "informacoes_complementares", None)

    desfecho = None
    if getattr(parecer_model, "desfecho", None) is not None:
        desfecho = getattr(parecer_model.desfecho, "desfecho", None)

    parecer_id = parecer_model.id
    lista_objetos_cid = ParecerCID.get_list_cid(parecer_id)
    cid_list = [c.codigo for c in lista_objetos_cid if getattr(c, "codigo", None)]

    lista_objetos = ParecerObjeto.get_list_objetos(parecer_id)
    objeto_textos = [obj.nome for obj in lista_objetos if getattr(obj, "nome", None)]

    lista_classificador = ParecerClassificador.get_list_classificadores(parecer_id)
    classificador_textos = [cls.nome for cls in lista_classificador if getattr(cls, "nome", None)]

    lista_insumos = ParecerInsumo.get_list_insumos(parecer_id)
    lista_medicamentos = ParecerMedicamento.get_list_medicamentos(parecer_id)
    medicamento_e_insumo = [f"{med.principio_ativo} - {med.nome}: {med.apresentacao}" for med in lista_medicamentos if getattr(med, "nome", None)]
    medicamento_e_insumo += [insumo.descricao for insumo in lista_insumos if getattr(insumo, "descricao", None)]

    documento = {
        "source_filename": nome_arquivo,
        "tipo_arquivo": os.path.splitext(nome_arquivo)[1].lower(),
        "processo": processo,
        "cid": cid_list,
        "n_nota_tecnica": nota_tecnica,
        "desfecho": desfecho,
        "inteiro_teor": texto,
        "objeto": objeto_textos,
        "classificador_do_objeto": classificador_textos,
        "informacao_complementar": informacao_complementar,
        "data_do_envio": data_do_envio,
        "medicamento_e_insumo": medicamento_e_insumo
    }

    print(">>> INICIANDO INDEXAÇÃO NO ELASTIC")
    try:
        response = cliente.index(index=NOME_INDICE, document=documento)
        print(">>> INDEXAÇÃO CONCLUÍDA COM SUCESSO")
        return response
    except Exception as e:
        print(f">>> ERRO REAL AO INDEXAR: {e}")
        raise
