# libraries
import calendar
from datetime import datetime, timedelta
from collections import defaultdict
import statistics
from google.cloud import bigquery
from google.oauth2 import service_account

import os
import matplotlib.pyplot as plt
import seaborn as sns

# map de meses en inglés a español
month_map = {
    1: 'Enero',
    2: 'Febrero',   
    3: 'Marzo',
    4: 'Abril',
    5: 'Mayo',
    6: 'Junio',
    7: 'Julio',
    8: 'Agosto',
    9: 'Septiembre',
    10: 'Octubre',
    11: 'Noviembre',
    12: 'Diciembre'
}


# BigQuery Configuration
PROJECT_ID = 'neat-chain-450900-a1'
DATASET_VENTAS_ID = 'Ventas'

# LOCAL PATH FOR SERVICE ACCOUNT FILE
SERVICE_ACCOUNT_FILE = r'C:\Users\gcbso\Downloads\EMPRESA GCB\REFUGIO PROYECTO PYTHON LOCAL\neat-chain-450900-a1-f67bb083925b.json'
# N8N PATH FOR SERVICE ACCOUNT FILE
# SERVICE_ACCOUNT_FILE = '/data/scripts/neat-chain-450900-a1-f67bb083925b.json'

# LOCAL PATH SAVE REPORTS
OUTPUT_FOLDER = 'reportes'
# N8N PATH SAVE REPORTS
# OUTPUT_FOLDER = '/data/scripts/reportes'

TABLE_VENTAS_ID = 'sales_df'
TABLE_CATEGORIAS_ID = 'categorias'
TABLE_METAS_ID = 'MontosMeta'
TABLE_NEGOCIOS_ID = 'Negocios'

# Definir intervalo semanal manualmente
# FECHA_FIN = datetime.today().date()  
# FECHA_INICIO = FECHA_FIN - timedelta(days=6) 
# FECHA_INICIO = '2025-06-09'
# FECHA_FIN = '2025-06-15'
 
 # FECHA_FIN es el último día de la semana (domingo) y FECHA_INICIO es el primer día de la semana (lunes), cálculo automático
FECHA_FIN = datetime.today().date()
FECHA_INICIO = FECHA_FIN - timedelta(days=6)


LOCATARIO_EXCLUIDO = 'Bar Refugio'

# BigQuery client
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
bigquery_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)


# Función para verfiicar si la tabla sales_df existe
def visualizar_tabla_sales_df():
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_VENTAS_ID}.sales_df` LIMIT 100"
    try:
        df = bigquery_client.query(query).to_dataframe()
        print("Tabla 'sales_df':")
        print(df.head())  # Mostrar las primeras filas
    except Exception as e:
        print(f"Error al consultar la tabla 'sales_df': {e}")


def obtener_ventas_semanales():
    query = f"""
        SELECT 
            n.Descripcion AS nombre_restaurante,
            SUM(CAST(s.Monto AS FLOAT64)) AS total_ventas
        FROM `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_VENTAS_ID}` s
        JOIN `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_NEGOCIOS_ID}` n
            ON s.CodigoNegocio = n.CodigoNegocio
        WHERE DATE(s.Fecha) BETWEEN DATE('{FECHA_INICIO}') AND DATE('{FECHA_FIN}')
             AND Estado = '0.0'
        GROUP BY nombre_restaurante
    """
    df = bigquery_client.query(query).to_dataframe()
    return df

def calcular_ranking(df):
    df = df.sort_values('total_ventas', ascending=False).reset_index(drop=True)
    df['ranking'] = df.index + 1
    return df

def graficar_ranking(df, restaurante, output_folder='reportes'):
   
    os.makedirs(output_folder, exist_ok=True)
    # Solo nombres y posiciones
    plot_df = df[['nombre_restaurante', 'ranking']].copy()
    plot_df = plot_df.sort_values('ranking')
    plt.figure(figsize=(8, max(4, len(plot_df)*0.5)))
    sns.set_style('whitegrid')
    ax = sns.barplot(
        x='ranking', y='nombre_restaurante', data=plot_df,
        palette=['#1f77b4' if n != restaurante else '#ff7f0e' for n in plot_df['nombre_restaurante']]
    )
    plt.title('Ranking de Ventas Refugio (Sin Valores)', fontsize=16, weight='bold')
    plt.xlabel('Posición')
    plt.ylabel('Restaurante')
    # Mostrar solo la posición, no el valor
    for i, (pos, name) in enumerate(zip(plot_df['ranking'], plot_df['nombre_restaurante'])):
        ax.text(pos, i, f"#{pos}", va='center', ha='left', fontsize=10, color='black')
    plt.tight_layout()
    plt.savefig(f"{output_folder}/ranking_{restaurante}.png")
    plt.close()
 
def generar_reporte_ranking_global():
    df_ventas = obtener_ventas_semanales()
    
    df_ventas = df_ventas[df_ventas['nombre_restaurante'] != LOCATARIO_EXCLUIDO]
    
    df_ranking = calcular_ranking(df_ventas)
    # Gráfico de barras horizontal descendente, el mayor primero
    plt.figure(figsize=(10, max(4, len(df_ranking)*0.5)))
    sns.set_style('whitegrid')
    ax = sns.barplot(
        x='total_ventas', y='nombre_restaurante', data=df_ranking,
        order=df_ranking.sort_values('total_ventas', ascending=False)['nombre_restaurante'],
        hue='nombre_restaurante',
        palette='Blues_d', 
        legend=False
    )
    
    day_inicio = datetime.strptime(FECHA_INICIO, '%Y-%m-%d').strftime('%d')
    day_fin = datetime.strptime(FECHA_FIN, '%Y-%m-%d').strftime('%d')
    month_inicio = month_map[datetime.strptime(FECHA_INICIO, '%Y-%m-%d').month]
    month_fin = month_map[datetime.strptime(FECHA_FIN, '%Y-%m-%d').month]    
    subititle = ''
    if month_inicio == month_fin:
        subititle = f"Del {day_inicio} al {day_fin} de {month_inicio} de 2025"
    else:
        subititle = f"Del {day_inicio} de {month_inicio} al {day_fin} de {month_fin} de 2025" 
    plt.title('Ranking Global de Ventas Semanal'
                f"\n{subititle}", fontsize=16, weight='bold')
    
    plt.xlabel('Puesto en el raking en base al Total de Ventas')
    plt.ylabel('Locatarios')
    # Mostrar solo la posición, no el valor
    for i, (ventas, name) in enumerate(zip(df_ranking['total_ventas'], df_ranking['nombre_restaurante'])):
        ax.text(ventas, i, f"#{i+1}", va='center', ha='left', fontsize=10, color='black')
    ax.set_xticks([])  # Oculta los valores del eje x
    plt.tight_layout()
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    plt.savefig(f"{OUTPUT_FOLDER}/ranking_global_semanal.png")
    plt.close()
    print('Reporte global generado en ', OUTPUT_FOLDER)


if __name__ == "__main__":
    #visualizar_tabla_sales_df()
    generar_reporte_ranking_global()
