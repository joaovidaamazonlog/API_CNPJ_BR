from fastapi import FastAPI, HTTPException
import sqlite3
import os

app = FastAPI()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '/prospeccao.db')

@app.get("/")
def home():
    return {"status": "API de Prospecção Ativa", "versao": "1.0"}

    @app.get("/buscar")
    def buscar_por_cep(cep: str):
        # Verifica se o arquivo do banco existe
            if not os.path.exists(DB_PATH):
                    raise HTTPException(status_code=500, detail="Banco de dados não encontrado.")
                        
                            # Remove traços do CEP caso o usuário envie "01001-000"
                                cep_limpo = cep.replace("-", "").strip()

                                    # Conecta no SQLite
                                        conn = sqlite3.connect(DB_PATH)
                                            conn.row_factory = sqlite3.Row # Faz o SQLite retornar os dados como um dicionário (JSON)
                                                cursor = conn.cursor()
                                                    
                                                        # Executa a busca no banco
                                                            # IMPORTANTE: A tabela tem que ter o mesmo nome que você definiu no script de ETL (empresas_alvo)
                                                                cursor.execute("SELECT * FROM empresas_alvo WHERE cep = ?", (cep_limpo,))
                                                                    resultados = cursor.fetchall()
                                                                        conn.close()
                                                                            
                                                                                # Se não achar nada, retorna lista vazia
                                                                                    if not resultados:
                                                                                            return {"total": 0, "empresas": []}
                                                                                                    
                                                                                                        # Converte os resultados para uma lista pronta para ser enviada
                                                                                                            lista_empresas = [dict(row) for row in resultados]
                                                                                                                
                                                                                                                    return {
                                                                                                                            "total": len(lista_empresas),
                                                                                                                                    "empresas": lista_empresas
                                                                                                                                        }
handler = app
