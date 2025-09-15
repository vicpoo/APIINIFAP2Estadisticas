from fastapi import FastAPI, HTTPException
import mysql.connector
from mysql.connector import Error
import pandas as pd
from scipy import stats
from typing import Dict, Any, List, Optional
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

app = FastAPI()

# Configuración de la base de datos
def get_db_connection():
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            database=os.getenv('DB_NAME', 'inifap_db'),
            user=os.getenv('DB_USER', 'root'),
            password=os.getenv('DB_PASSWORD', '')
        )
        return connection
    except Error as e:
        raise HTTPException(status_code=500, detail=f"Error connecting to database: {e}")

@app.get("/")
def read_root():
    return {"mensaje": "Hola FastAPI desde CMD 🚀"}

@app.get("/saludo/{nombre}")
def read_item(nombre: str):
    return {"saludo": f"Hola {nombre}, bienvenido a FastAPI"}

@app.get("/estadisticas/municipio/{municipio_id}")
def get_estadisticas_municipio_por_id(municipio_id: int):
    """
    Obtiene estadísticas de análisis químicos por ID de municipio
    """
    return obtener_estadisticas_municipio("municipio_id_FK", municipio_id)

@app.get("/estadisticas/municipio/nombre/{municipio_nombre}")
def get_estadisticas_municipio_por_nombre(municipio_nombre: str):
    """
    Obtiene estadísticas de análisis químicos por nombre de municipio
    """
    return obtener_estadisticas_municipio("municipio", municipio_nombre)

def obtener_estadisticas_municipio(campo: str, valor: Any):
    """
    Función auxiliar para obtener estadísticas por municipio
    """
    connection = None
    try:
        connection = get_db_connection()
        
        # Columnas para análisis estadístico
        columnas_estadisticas = [
            'arcilla', 'limo', 'arena', 'da', 'ph', 'mo', 'fosforo', 
            'n_inorganico', 'k', 'mg', 'ca', 'na', 'al', 'cic', 
            'cic_calculada', 'h', 'azufre', 'hierro', 'cobre', 
            'zinc', 'manganeso', 'boro', 'ca_mg', 'mg_k', 'ca_k', 
            'ca_mg_k', 'k_mg'
        ]
        
        # Consulta SQL
        query = f"""
        SELECT {', '.join(columnas_estadisticas)} 
        FROM analisis_quimicos_validados 
        WHERE {campo} = %s
        """
        
        # Leer datos en DataFrame
        df = pd.read_sql(query, connection, params=[valor])
        
        if df.empty:
            return {
                "municipio_id" if campo == "municipio_id_FK" else "municipio_nombre": valor, 
                "mensaje": "No hay datos para este municipio", 
                "estadisticas": {}
            }
        
        # Obtener información adicional del municipio
        info_query = f"""
        SELECT municipio_id_FK, municipio 
        FROM analisis_quimicos_validados 
        WHERE {campo} = %s 
        LIMIT 1
        """
        info_df = pd.read_sql(info_query, connection, params=[valor])
        
        municipio_id = int(info_df['municipio_id_FK'].iloc[0]) if 'municipio_id_FK' in info_df.columns else None
        municipio_nombre = info_df['municipio'].iloc[0] if 'municipio' in info_df.columns else None
        
        # Calcular estadísticas para cada columna
        estadisticas = {}
        
        for columna in columnas_estadisticas:
            if columna in df.columns:
                datos = df[columna].dropna()  # Eliminar valores nulos
                
                if len(datos) > 0:
                    try:
                        # Calcular moda de manera segura
                        if len(datos) > 1:
                            moda_result = stats.mode(datos, keepdims=True)
                            moda = float(moda_result.mode[0]) if moda_result.count[0] > 1 else float(datos.iloc[0])
                        else:
                            moda = float(datos.iloc[0])
                            
                        estadisticas[columna] = {
                            "moda": moda,
                            "mediana": float(datos.median()),
                            "media": float(datos.mean()),
                            "sesgo": float(datos.skew()),
                            "desviacion_estandar": float(datos.std()),
                            "maximo": float(datos.max()),
                            "minimo": float(datos.min()),
                            "count": int(len(datos)),
                            "q1": float(datos.quantile(0.25)),
                            "q3": float(datos.quantile(0.75))
                        }
                    except Exception as e:
                        estadisticas[columna] = {"error": f"No se pudieron calcular estadísticas: {str(e)}"}
                else:
                    estadisticas[columna] = {"mensaje": "No hay datos válidos para calcular estadísticas"}
        
        resultado = {
            "municipio_id": municipio_id,
            "municipio_nombre": municipio_nombre,
            "total_registros": int(len(df)),
            "estadisticas": estadisticas
        }
        
        # Si buscamos por nombre, incluir el ID, y viceversa
        if campo == "municipio":
            resultado["municipio_id"] = municipio_id
        elif campo == "municipio_id_FK":
            resultado["municipio_nombre"] = municipio_nombre
            
        return resultado
        
    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        if connection and connection.is_connected():
            connection.close()

@app.get("/estadisticas/municipios")
def get_estadisticas_todos_municipios():
    """
    Obtiene estadísticas para todos los municipios
    """
    connection = None
    try:
        connection = get_db_connection()
        
        # Obtener lista de municipios con datos
        query_municipios = """
        SELECT DISTINCT municipio_id_FK, municipio 
        FROM analisis_quimicos_validados 
        ORDER BY municipio
        """
        
        municipios_df = pd.read_sql(query_municipios, connection)
        
        resultados = []
        
        for _, row in municipios_df.iterrows():
            municipio_id = row['municipio_id_FK']
            municipio_nombre = row['municipio']
            
            # Agregar URLs para ambos métodos de acceso
            resultados.append({
                "municipio_id": int(municipio_id),
                "municipio_nombre": municipio_nombre,
                "estadisticas_por_id_url": f"/estadisticas/municipio/{municipio_id}",
                "estadisticas_por_nombre_url": f"/estadisticas/municipio/nombre/{municipio_nombre.replace(' ', '%20')}"
            })
        
        return {
            "total_municipios": len(resultados),
            "municipios": resultados
        }
        
    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        if connection and connection.is_connected():
            connection.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)