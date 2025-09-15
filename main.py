from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
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

# Configurar CORS para permitir cualquier frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite todos los orígenes
    allow_credentials=True,
    allow_methods=["*"],  # Permite todos los métodos
    allow_headers=["*"],  # Permite todos los headers
)

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

# Diccionario de valores de referencia para interpretación
VALORES_REFERENCIA = {
    'ph': {
        'bajo': {'max': 5.5, 'descripcion': 'ácido'},
        'medio': {'min': 6.0, 'max': 7.0, 'descripcion': 'óptimo'},
        'alto': {'min': 7.5, 'descripcion': 'alcalino'}
    },
    'mo': {
        'bajo': {'max': 2.0, 'descripcion': '< 2%'},
        'medio': {'min': 2.0, 'max': 4.0, 'descripcion': '2-4%'},
        'alto': {'min': 4.0, 'descripcion': '> 4%'}
    },
    'n_inorganico': {
        'bajo': {'max': 15.0, 'descripcion': '< 15 mg/kg'},
        'medio': {'min': 16.0, 'max': 29.0, 'descripcion': '16-29 mg/kg'},
        'alto': {'min': 30.0, 'descripcion': '> 30 mg/kg'}
    },
    'fosforo': {
        'bajo': {'max': 20.0, 'descripcion': '< 20 mg/kg'},
        'medio': {'min': 20.0, 'max': 40.0, 'descripcion': '20-40 mg/kg'},
        'alto': {'min': 40.0, 'descripcion': '> 40 mg/kg'}
    },
    'k': {
        'bajo': {'max': 0.3, 'descripcion': '< 0.3 cmol/kg'},
        'medio': {'min': 0.3, 'max': 0.6, 'descripcion': '0.3-0.6 cmol/kg'},
        'alto': {'min': 0.6, 'descripcion': '> 0.6 cmol/kg'}
    },
    'mg': {
        'bajo': {'max': 0.2, 'descripcion': '< 0.2 cmol/kg'},
        'medio': {'min': 0.2, 'max': 0.5, 'descripcion': '0.2-0.5 cmol/kg'},
        'alto': {'min': 0.5, 'descripcion': '> 0.5 cmol/kg'}
    },
    'ca': {
        'bajo': {'max': 2.0, 'descripcion': '< 2 cmol/kg'},
        'medio': {'min': 2.0, 'max': 5.0, 'descripcion': '2-5 cmol/kg'},
        'alto': {'min': 5.0, 'descripcion': '> 5 cmol/kg'}
    },
    'azufre': {
        'bajo': {'max': 10.0, 'descripcion': '< 10 mg/kg'},
        'medio': {'min': 10.0, 'max': 20.0, 'descripcion': '10-20 mg/kg'},
        'alto': {'min': 20.0, 'descripcion': '> 20 mg/kg'}
    },
    'hierro': {
        'bajo': {'max': 10.0, 'descripcion': '< 10 mg/kg'},
        'medio': {'min': 10.0, 'max': 40.0, 'descripcion': '10-40 mg/kg'},
        'alto': {'min': 40.0, 'descripcion': '> 40 mg/kg'}
    },
    'cobre': {
        'bajo': {'max': 0.5, 'descripcion': '< 0.5 mg/kg'},
        'medio': {'min': 0.5, 'max': 1.0, 'descripcion': '0.5-1 mg/kg'},
        'alto': {'min': 1.0, 'descripcion': '> 1 mg/kg'}
    },
    'zinc': {
        'bajo': {'max': 1.0, 'descripcion': '< 1 mg/kg'},
        'medio': {'min': 1.0, 'max': 3.0, 'descripcion': '1-3 mg/kg'},
        'alto': {'min': 3.0, 'descripcion': '> 3 mg/kg'}
    },
    'manganeso': {
        'bajo': {'max': 5.0, 'descripcion': '< 5 mg/kg'},
        'medio': {'min': 5.0, 'max': 20.0, 'descripcion': '5-20 mg/kg'},
        'alto': {'min': 20.0, 'descripcion': '> 20 mg/kg'}
    },
    'boro': {
        'bajo': {'max': 0.5, 'descripcion': '< 0.5 mg/kg'},
        'medio': {'min': 0.5, 'max': 1.0, 'descripcion': '0.5-1 mg/kg'},
        'alto': {'min': 1.0, 'descripcion': '> 1 mg/kg'}
    },
    'cic': {
        'bajo': {'max': 10.0, 'descripcion': 'suelo pobre'},
        'medio': {'min': 10.0, 'max': 25.0, 'descripcion': 'normal'},
        'alto': {'min': 25.0, 'descripcion': 'suelo rico'}
    }
}

def interpretar_valor(parametro: str, valor: float) -> Dict[str, Any]:
    """
    Interpreta un valor comparándolo con los rangos de referencia
    """
    if parametro not in VALORES_REFERENCIA:
        return {
            'valor': valor,
            'interpretacion': 'No hay referencia disponible',
            'nivel': 'sin_referencia',
            'descripcion': 'Parámetro sin valores de referencia definidos'
        }
    
    ref = VALORES_REFERENCIA[parametro]
    valor = float(valor)
    
    # Verificar rango bajo
    if 'max' in ref['bajo'] and valor <= ref['bajo']['max']:
        return {
            'valor': valor,
            'interpretacion': 'BAJO',
            'nivel': 'bajo',
            'descripcion': ref['bajo']['descripcion']
        }
    
    # Verificar rango alto
    if 'min' in ref['alto'] and valor >= ref['alto']['min']:
        return {
            'valor': valor,
            'interpretacion': 'ALTO',
            'nivel': 'alto',
            'descripcion': ref['alto']['descripcion']
        }
    
    # Verificar rango medio (si existe)
    if 'medio' in ref and 'min' in ref['medio'] and 'max' in ref['medio']:
        if ref['medio']['min'] <= valor <= ref['medio']['max']:
            return {
                'valor': valor,
                'interpretacion': 'MEDIO',
                'nivel': 'medio',
                'descripcion': ref['medio']['descripcion']
            }
    
    # Si no está en ningún rango definido
    return {
        'valor': valor,
        'interpretacion': 'FUERA DE RANGO',
        'nivel': 'fuera_rango',
        'descripcion': 'Valor no se encuentra en los rangos definidos'
    }

@app.get("/")
def read_root():
    return {"mensaje": "Hola FastAPI desde CMD 🚀"}

@app.get("/saludo/{nombre}")
def read_item(nombre: str):
    return {"saludo": f"Hola {nombre}, bienvenido a FastAPI"}

@app.get("/interpretacion/municipio/{municipio_id}")
def get_interpretacion_municipio_por_id(municipio_id: int):
    """
    Obtiene interpretación de análisis químicos por ID de municipio
    """
    return obtener_interpretacion_municipio("municipio_id_FK", municipio_id)

@app.get("/interpretacion/municipio/nombre/{municipio_nombre}")
def get_interpretacion_municipio_por_nombre(municipio_nombre: str):
    """
    Obtiene interpretación de análisis químicos por nombre de municipio
    """
    return obtener_interpretacion_municipio("municipio", municipio_nombre)

def obtener_interpretacion_municipio(campo: str, valor: Any):
    """
    Función auxiliar para obtener interpretación por municipio
    """
    connection = None
    try:
        connection = get_db_connection()
        
        # Columnas para análisis estadístico
        columnas_interpretacion = list(VALORES_REFERENCIA.keys())
        
        # Consulta SQL
        query = f"""
        SELECT {', '.join(columnas_interpretacion)} 
        FROM analisis_quimicos_validados 
        WHERE {campo} = %s
        """
        
        # Leer datos en DataFrame
        df = pd.read_sql(query, connection, params=[valor])
        
        if df.empty:
            return {
                "municipio_id" if campo == "municipio_id_FK" else "municipio_nombre": valor, 
                "mensaje": "No hay datos para este municipio", 
                "interpretaciones": {}
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
        
        # Calcular medianas e interpretaciones para cada parámetro
        interpretaciones = {}
        
        for parametro in columnas_interpretacion:
            if parametro in df.columns:
                datos = df[parametro].dropna()  # Eliminar valores nulos
                
                if len(datos) > 0:
                    try:
                        mediana = float(datos.median())
                        interpretacion = interpretar_valor(parametro, mediana)
                        
                        interpretaciones[parametro] = {
                            'mediana': mediana,
                            'interpretacion': interpretacion['interpretacion'],
                            'nivel': interpretacion['nivel'],
                            'descripcion': interpretacion['descripcion'],
                            'muestras_validas': int(len(datos))
                        }
                    except Exception as e:
                        interpretaciones[parametro] = {
                            "error": f"No se pudo interpretar: {str(e)}",
                            "mediana": None
                        }
                else:
                    interpretaciones[parametro] = {
                        "mensaje": "No hay datos válidos",
                        "mediana": None
                    }
        
        resultado = {
            "municipio_id": municipio_id,
            "municipio_nombre": municipio_nombre,
            "total_registros": int(len(df)),
            "interpretaciones": interpretaciones
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
                "estadisticas_por_nombre_url": f"/estadisticas/municipio/nombre/{municipio_nombre.replace(' ', '%20')}",
                "interpretacion_por_id_url": f"/interpretacion/municipio/{municipio_id}",
                "interpretacion_por_nombre_url": f"/interpretacion/municipio/nombre/{municipio_nombre.replace(' ', '%20')}"
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