# Importa as bibliotecas necessárias
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import psycopg2
import psycopg2.extras
import json
from typing import Optional

# --- CONFIGURE AQUI A SUA CONEXÃO COM O BANCO ---
DB_HOST = "localhost"
DB_NAME = "icetrack_bd"  # O nome do seu banco de dados
DB_USER = "postgres"     # Seu usuário do PostgreSQL
DB_PASS = "admin"     # <<<<<<< ATUALIZE COM SUA SENHA SE FOR DIFERENTE
# -------------------------------------------------

# Cria a aplicação FastAPI
app = FastAPI()

# Configura o CORS para permitir que o seu HTML se comunique com este servidor
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite todas as origens (para desenvolvimento)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Função para conectar ao banco de dados
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS
        )
        return conn
    except psycopg2.OperationalError as e:
        raise HTTPException(status_code=500, detail=f"Não foi possível conectar ao banco de dados: {e}")

# Função genérica para executar consultas
def execute_query(query: str, params: Optional[dict] = None, fetch_one=True):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(query, params)
            if fetch_one:
                return cur.fetchone()
            return cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao executar a consulta: {e}")
    finally:
        conn.close()

# --- ENDPOINTS DA API ---

@app.get("/api/estatisticas_globais")
def get_estatisticas_globais():
    """Endpoint para o dashboard com estatísticas globais."""
    query = "SELECT * FROM estatisticas_globais;"
    result = execute_query(query)
    return dict(result) if result else {}

@app.get("/api/geleiras_filtradas")
def get_geleiras_filtradas(ano_inicial: int, ano_final: int, continente: Optional[str] = None, pais: Optional[str] = None):
    """Busca geleiras com base em um período e, opcionalmente, por continente ou país."""
    
    # CORREÇÃO: A lógica da consulta foi reestruturada para construir a subconsulta corretamente.
    
    # A subconsulta agora começa com 'SELECT *' para ser sintaticamente válida.
    subquery = "SELECT * FROM public.resumo_taxa_recuo_geleiras r"
    
    where_clauses = [
        "r.geom IS NOT NULL",
        "r.mudanca_percentual > -100",
        "EXISTS (SELECT 1 FROM public.analise_final_geleiras a WHERE a.glac_id = r.glac_id AND EXTRACT(YEAR FROM a.data_observacao) BETWEEN %(ano_inicial)s AND %(ano_final)s)"
    ]
    params = {'ano_inicial': ano_inicial, 'ano_final': ano_final}

    # Adiciona filtros geográficos se fornecidos
    if pais and pais != 'Todos':
        where_clauses.append('r.pais = %(pais)s')
        params['pais'] = pais
    elif continente and continente != 'Todos':
        where_clauses.append('r.pais IN (SELECT "ADMIN" FROM public.paises WHERE "CONTINENT" = %(continente)s)')
        params['continente'] = continente
    
    # Constrói a cláusula WHERE
    if where_clauses:
        subquery += " WHERE " + " AND ".join(where_clauses)

    # Se nenhum filtro geográfico for aplicado, ordena e limita aos 100 maiores recuos.
    if (not pais or pais == 'Todos') and (not continente or continente == 'Todos'):
        subquery += " ORDER BY r.taxa_recuo_anual_km2 ASC LIMIT 100"
        
    # A consulta final agora envolve a subconsulta corretamente construída.
    query = f"""
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(
                json_build_object(
                    'type', 'Feature',
                    'id', r.glac_id,
                    'properties', row_to_json(r),
                    'geometry', ST_AsGeoJSON(r.geom)::json
                )
            )
        ) AS geojson
        FROM ({subquery}) AS r;
    """

    result = execute_query(query, params)
    return result['geojson'] if result and result['geojson'] else {"type": "FeatureCollection", "features": []}


@app.get("/api/buscar_geleira")
def buscar_geleira(nome: str):
    """Endpoint para a busca de geleiras por nome."""
    query = """
        SELECT json_build_object('type', 'FeatureCollection', 'features', json_agg(json_build_object('type', 'Feature', 'properties', row_to_json(t), 'geometry', ST_AsGeoJSON(t.geom)::json))) AS geojson
        FROM (
            SELECT * FROM resumo_taxa_recuo_geleiras
            WHERE glac_name ILIKE %(nome)s AND geom IS NOT NULL
            LIMIT 10
        ) AS t;
    """
    result = execute_query(query, {'nome': f'%{nome}%'})
    return result['geojson'] if result and result['geojson'] else {"type": "FeatureCollection", "features": []}

@app.get("/api/lista_paises")
def get_lista_paises():
    """Endpoint para obter a lista de países que têm geleiras."""
    query = "SELECT DISTINCT pais FROM public.resumo_taxa_recuo_geleiras WHERE pais IS NOT NULL ORDER BY pais;"
    results = execute_query(query, fetch_one=False)
    return [row['pais'] for row in results] if results else []

@app.get("/api/historico_completo")
def get_historico_data():
    """Endpoint para o histórico completo (necessário para o painel de detalhes)."""
    query = "SELECT json_object_agg(glac_id, historico) AS json_data FROM (SELECT glac_id, json_agg(json_build_object('data_observacao', data_observacao, 'area_km2_calculada', area_km2_calculada) ORDER BY data_observacao) AS historico FROM historico_detalhado_geleiras GROUP BY glac_id) AS t;"
    result = execute_query(query)
    return result['json_data'] if result and result['json_data'] else {}

@app.get("/api/estatisticas_dinamicas")
async def get_estatisticas_dinamicas(
    ano_inicial: int,
    ano_final: int,
    pais: Optional[str] = None,
    continente: Optional[str] = None
):
    query = """
        SELECT
            COUNT(glac_id) AS total_geleiras_analisadas,
            SUM(mudanca_total_km2) AS perda_total_global,
            AVG(mudanca_total_km2 / NULLIF(intervalo_anos, 0)) AS media_recuo_anual
        FROM
            public.resumo_recuo_geleiras
        WHERE
            -- Filtro de intervalo de tempo, sempre aplicado
            intervalo_anos > 0 AND
            primeira_data >= TO_DATE(%(ano_inicial)s::text, 'YYYY') AND
            ultima_data <= TO_DATE(%(ano_final)s::text, 'YYYY')
    """
    params = {'ano_inicial': ano_inicial, 'ano_final': ano_final}

    # Adiciona filtros dinâmicos se eles forem fornecidos e não forem "Todos"
    if pais and pais != "Todos":
        query += " AND pais = %(pais)s"
        params['pais'] = pais
    if continente and continente != "Todos":
        pass 
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(query, params)
        resultado = cur.fetchone()
        cur.close()
        return resultado
    except Exception as e:
        print(f"Erro na base de dados: {e}")
        raise HTTPException(status_code=500, detail="Erro ao buscar estatísticas")
    finally:
        if conn:
            conn.close()