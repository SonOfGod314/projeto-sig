# Importa as bibliotecas necessárias
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import psycopg2.extras
from psycopg2.extras import RealDictCursor
from typing import Optional

# --- CONFIGURE AQUI A SUA CONEXÃO COM O BANCO ---
DB_HOST = "localhost"
DB_NAME = "icetrack"
DB_USER = "postgres"
DB_PASS = "postgres"
# -------------------------------------------------

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS
        )
        return conn
    except psycopg2.OperationalError as e:
        raise HTTPException(status_code=500, detail=f"Não foi possível conectar ao banco de dados: {e}")

def execute_query(query: str, params: Optional[dict] = None, fetch_one=True):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            if fetch_one:
                return cur.fetchone()
            return cur.fetchall()
    except Exception as e:
        print(f"ERRO DE BANCO DE DADOS: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao executar a consulta: {e}")
    finally:
        conn.close()

# --- Endpoints da API ---

@app.get("/api/geleiras_filtradas")
def get_geleiras_filtradas(ano_inicial: int, ano_final: int, continente: Optional[str] = None, pais: Optional[str] = None):
    subquery = "SELECT * FROM public.resumo_taxa_recuo_geleiras r"
    where_clauses = [
        "r.geom IS NOT NULL",
        "r.mudanca_percentual > -100",
        "EXISTS (SELECT 1 FROM public.analise_final_geleiras a WHERE a.glac_id = r.glac_id AND EXTRACT(YEAR FROM a.data_observacao) BETWEEN %(ano_inicial)s AND %(ano_final)s)"
    ]
    params = {'ano_inicial': ano_inicial, 'ano_final': ano_final}
    if pais and pais != 'Todos':
        where_clauses.append('r.pais = %(pais)s')
        params['pais'] = pais
    elif continente and continente != 'Todos':
        where_clauses.append('r.pais IN (SELECT "ADMIN" FROM public.paises WHERE "CONTINENT" = %(continente)s)')
        params['continente'] = continente
    if where_clauses:
        subquery += " WHERE " + " AND ".join(where_clauses)
    if (not pais or pais == 'Todos') and (not continente or continente == 'Todos'):
        subquery += " ORDER BY r.taxa_recuo_anual_km2 ASC LIMIT 100"
    query = f"""
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(json_build_object('type', 'Feature', 'id', r.glac_id, 'properties', row_to_json(r), 'geometry', ST_AsGeoJSON(r.geom)::json))
        ) AS geojson FROM ({subquery}) AS r;
    """
    result = execute_query(query, params)
    return result['geojson'] if result and result['geojson'] else {"type": "FeatureCollection", "features": []}


@app.get("/api/buscar_geleira")
def buscar_geleira(nome: str):
    """Endpoint para a busca de geleiras por nome."""
    query = """
        SELECT json_build_object(
            'type', 'FeatureCollection', 
            'features', json_agg(
                json_build_object(
                    'type', 'Feature', 
                    'properties', row_to_json(t), 
                    'geometry', ST_AsGeoJSON(t.geom)::json
                )
            )
        ) AS geojson
        FROM (
            SELECT * FROM resumo_taxa_recuo_geleiras
            WHERE glac_name ILIKE %(nome)s AND geom IS NOT NULL
            LIMIT 10
        ) AS t;
    """
    # Adiciona os '%' para a busca parcial (LIKE)
    result = execute_query(query, {'nome': f'%{nome}%'})
    return result['geojson'] if result and result['geojson'] else {"type": "FeatureCollection", "features": []}

@app.get("/api/lista_paises")
def get_lista_paises():
    query = "SELECT DISTINCT pais FROM public.resumo_taxa_recuo_geleiras WHERE pais IS NOT NULL ORDER BY pais;"
    results = execute_query(query, fetch_one=False)
    return [row['pais'] for row in results] if results else []

@app.get("/api/historico_geleira/{glac_id}")
def get_historico_por_id(
    glac_id: str, 
    ano_inicial: Optional[int] = None, 
    ano_final: Optional[int] = None
):
    """
    Busca o histórico de uma geleira, aplicando média anual e suavização 
    para corrigir dados discrepantes.
    """
    
    params = {'glac_id': glac_id}
    filter_clauses = ["glac_id = %(glac_id)s"]

    if ano_inicial is not None and ano_final is not None:
        filter_clauses.append("EXTRACT(YEAR FROM data_observacao) BETWEEN %(ano_inicial)s AND %(ano_final)s")
        params['ano_inicial'] = ano_inicial
        params['ano_final'] = ano_final

    filter_sql = " AND ".join(filter_clauses)

    query = f"""
        WITH dados_agrupados_por_ano AS (
            -- Etapa 1: Agrupa múltiplas observações no mesmo ano, calculando a média da área.
            SELECT
                EXTRACT(YEAR FROM data_observacao) AS ano,
                AVG(area_km2_calculada) AS area_media
            FROM historico_detalhado_geleiras
            WHERE {filter_sql}
            GROUP BY EXTRACT(YEAR FROM data_observacao)
        ),
        dados_com_vizinhos AS (
            -- Etapa 2: Pega a área média do ano anterior e do próximo.
            SELECT
                ano,
                area_media,
                LAG(area_media, 1) OVER (ORDER BY ano) as area_anterior,
                LEAD(area_media, 1) OVER (ORDER BY ano) as area_seguinte
            FROM dados_agrupados_por_ano
        ),
        dados_suavizados AS (
            -- Etapa 3: Substitui valores anômalos (área <= 0) pela média dos vizinhos.
            SELECT
                -- Cria uma data representativa para o ano, para o gráfico.
                make_date(ano::integer, 1, 1) as data_observacao,
                CASE
                    WHEN area_media <= 0 AND area_anterior > 0 AND area_seguinte > 0
                    THEN (area_anterior + area_seguinte) / 2
                    ELSE area_media
                END as area_km2_calculada
            FROM dados_com_vizinhos
        )
        -- Etapa Final: Agrupa os dados limpos em um JSON.
        SELECT 
            %(glac_id)s as glac_id,
            json_agg(
                json_build_object(
                    'data_observacao', data_observacao, 
                    'area_km2_calculada', area_km2_calculada
                ) ORDER BY data_observacao
            ) AS historico
        FROM dados_suavizados
        WHERE area_km2_calculada > 0; -- Garante que nenhum zero remanescente seja incluído.
    """
    
    result = execute_query(query, params)
    return result['historico'] if result and result['historico'] else []

@app.get("/api/estatisticas_dinamicas")
async def get_estatisticas_dinamicas(
    ano_inicial: int,
    ano_final: int,
    pais: Optional[str] = None,
    continente: Optional[str] = None
):
    """Calcula as estatísticas dinâmicas usando a mesma lógica de filtragem do mapa."""
    where_clauses = [
        "intervalo_anos > 0",
        "EXISTS (SELECT 1 FROM public.analise_final_geleiras a WHERE a.glac_id = r.glac_id AND EXTRACT(YEAR FROM a.data_observacao) BETWEEN %(ano_inicial)s AND %(ano_final)s)"
    ]
    params = {'ano_inicial': ano_inicial, 'ano_final': ano_final}

    if pais and pais != "Todos":
        where_clauses.append("r.pais = %(pais)s")
        params['pais'] = pais
    elif continente and continente != "Todos":
        where_clauses.append("r.pais IN (SELECT \"ADMIN\" FROM public.paises WHERE \"CONTINENT\" = %(continente)s)")
        params['continente'] = continente

    where_sql = " AND ".join(where_clauses)

    query_final = f"""
        SELECT
            COUNT(r.glac_id) AS total_geleiras_analisadas,
            SUM(r.mudanca_total_km2) AS perda_total_global,
            AVG(r.taxa_recuo_anual_km2) AS media_recuo_anual
        FROM
            public.resumo_taxa_recuo_geleiras AS r
        WHERE
            {where_sql}
    """
    
    resultado = execute_query(query_final, params)
    
    if not resultado:
        return {"total_geleiras_analisadas": 0, "perda_total_global": 0, "media_recuo_anual": 0}
    
    return {
        "total_geleiras_analisadas": resultado.get("total_geleiras_analisadas") or 0,
        "perda_total_global": resultado.get("perda_total_global") or 0,
        "media_recuo_anual": resultado.get("media_recuo_anual") or 0,
    }