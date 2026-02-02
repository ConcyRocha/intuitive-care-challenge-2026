import os
from typing import List, Optional, Dict, Any
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, text
import json
import typing
from fastapi.responses import Response


class PrettyJSONResponse(Response):
    media_type = "application/json"

    def render(self, content: typing.Any) -> bytes:
        return json.dumps(
            content,
            ensure_ascii=False,
            allow_nan=False,
            indent=4,       
            separators=(", ", ": "),
        ).encode("utf-8")

# ============================================================================
# CONFIGURAÇÃO DO AMBIENTE E BANCO DE DADOS
# ============================================================================


DB_PATH = os.path.join(os.getcwd(), "teste_intu.db")
SQLALCHEMY_DATABASE_URL = f"sqlite:///{DB_PATH}"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False}
)

app = FastAPI(
    title="API Despesas Operadoras - Teste Intu",
    description="API para consulta de dados financeiros de operadoras ANS.",
    version="1.0.0",
    default_response_class=PrettyJSONResponse

)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# MODELOS DE DADOS (PYDANTIC SCHEMAS)
# ============================================================================


class Operadora(BaseModel):
    cnpj: str
    razao_social: Optional[str]
    uf: Optional[str]
    modalidade: Optional[str]


class Despesa(BaseModel):
    data_referencia: str
    valor_despesa: float


class PaginatedResponse(BaseModel):
    data: List[Operadora]
    total: int
    page: int
    limit: int


class EstatisticasResponse(BaseModel):
    total_geral: float
    media_lancamento: float
    top_5_operadoras: List[Dict[str, Any]]
    distribuicao_uf: List[Dict[str, Any]]

# ============================================================================
# ROTAS DA API (ENDPOINTS)
# ============================================================================

@app.get("/")
def read_root():
    return RedirectResponse(url="/app")

@app.get("/api/operadoras", response_model=PaginatedResponse)
def list_operadoras(
    page: int = 1,
    limit: int = 10,
    search: Optional[str] = None
):
    offset = (page - 1) * limit

    with engine.connect() as conn:
        sql_base = "SELECT CAST(cnpj AS TEXT) as cnpj, razao_social, uf, modalidade FROM dim_operadoras"
        count_base = "SELECT COUNT(*) FROM dim_operadoras"
        
        params = {"limit": limit, "offset": offset}
        where_clause = ""

        if search:
            where_clause = (
                " WHERE razao_social LIKE :search OR CAST(cnpj AS TEXT) LIKE :search"
            )
            params["search"] = f"%{search}%"

        query_count = text(f"{count_base}{where_clause}")
        total = conn.execute(query_count, params).scalar()

        query_data = text(
            f"{sql_base}{where_clause} "
            "ORDER BY razao_social LIMIT :limit OFFSET :offset"
        )
        result = conn.execute(query_data, params).mappings().all()

    return {
        "data": result,
        "total": total,
        "page": page,
        "limit": limit
    }


@app.get("/api/operadoras/{cnpj}", response_model=Operadora)
def get_operadora(cnpj: str):
    with engine.connect() as conn:
        # CORREÇÃO DE TIPO AQUI TAMBÉM
        query = text("""
            SELECT CAST(cnpj AS TEXT) as cnpj, razao_social, uf, modalidade 
            FROM dim_operadoras 
            WHERE CAST(cnpj AS TEXT) = :cnpj
        """)
        result = conn.execute(query, {"cnpj": cnpj}).mappings().first()

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"Operadora com CNPJ {cnpj} não encontrada."
        )

    return result


@app.get("/api/operadoras/{cnpj}/despesas", response_model=List[Despesa])
def get_historico_despesas(cnpj: str):
    with engine.connect() as conn:
        check_op = text("SELECT 1 FROM dim_operadoras WHERE CAST(cnpj AS TEXT) = :cnpj")
        exists = conn.execute(check_op, {"cnpj": cnpj}).first()

        if not exists:
            raise HTTPException(
                status_code=404,
                detail="Operadora não encontrada."
            )

        query = text("""
            SELECT data_referencia, valor_despesa
            FROM fact_despesas
            WHERE CAST(cnpj AS TEXT) = :cnpj
            ORDER BY data_referencia DESC
        """)
        result = conn.execute(query, {"cnpj": cnpj}).mappings().all()

    return result

@app.get("/api/estatisticas", response_model=EstatisticasResponse)
def get_estatisticas():
    with engine.connect() as conn:
        q_total = text("SELECT SUM(valor_despesa) FROM fact_despesas")
        total_despesas = conn.execute(q_total).scalar() or 0.0

        q_media = text("SELECT AVG(valor_despesa) FROM fact_despesas")
        media_despesas = conn.execute(q_media).scalar() or 0.0

        q_top5 = text("""
            SELECT o.razao_social, SUM(d.valor_despesa) as total
            FROM fact_despesas d
            JOIN dim_operadoras o ON d.cnpj = o.cnpj
            GROUP BY o.razao_social
            ORDER BY total DESC
            LIMIT 5
        """)
        top_5 = conn.execute(q_top5).mappings().all()

        q_uf = text("""
            SELECT o.uf, SUM(d.valor_despesa) as total
            FROM fact_despesas d
            JOIN dim_operadoras o ON d.cnpj = o.cnpj
            WHERE o.uf != 'Não Informado'
            GROUP BY o.uf
            ORDER BY total DESC
        """)
        dist_uf = conn.execute(q_uf).mappings().all()

    return {
        "total_geral": total_despesas,
        "media_lancamento": media_despesas,
        "top_5_operadoras": top_5,
        "distribuicao_uf": dist_uf
    }


app.mount("/app", StaticFiles(directory="frontend", html=True), name="frontend")

if __name__ == "__main__":
    uvicorn.run("api:app", host="127.0.0.1", port=8000, reload=True)