# libraries
import calendar
from datetime import datetime, timedelta
from collections import defaultdict
import statistics
from google.cloud import bigquery
from google.oauth2 import service_account

import os
import pandas as pd
import numpy as np
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
SERVICE_ACCOUNT_FILE = r'C:\Users\gcbso\Downloads\EMPRESA GCB\REFUGIO PROYECTO PYTHON LOCAL\neat-chain-450900-a1-f67bb083925b.json'

TABLE_VENTAS_ID = 'sales_df'
TABLE_CATEGORIAS_ID = 'categorias'
TABLE_METAS_ID = 'MontosMeta'
TABLE_NEGOCIOS_ID = 'Negocios'

# Definir intervalo semanal manualmente
# FECHA_FIN = datetime.today().date()  
# FECHA_INICIO = FECHA_FIN - timedelta(days=6) 
FECHA_INICIO = '2025-06-09'
FECHA_FIN = '2025-06-15'


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


RESTAURANTE_OBJETIVO = 'Anticuching'  # Cambia por el nombre del restaurante deseado


def obtener_ventas_semanales_anuales(restaurante):
    """Obtiene ventas semanales del año actual y anterior para un restaurante usando CodigoNegocio."""
    query = f'''
        WITH ventas AS (
            SELECT
                n.CodigoNegocio,
                n.Descripcion AS nombre_restaurante,
                EXTRACT(YEAR FROM DATE(s.Fecha)) AS anio,
                EXTRACT(WEEK FROM DATE(s.Fecha)) AS semana,
                DATE(s.Fecha) AS fecha,
                SUM(CAST(s.Monto AS FLOAT64)) AS total_ventas
            FROM `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_VENTAS_ID}` s
            JOIN `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_NEGOCIOS_ID}` n
                ON s.CodigoNegocio = n.CodigoNegocio
            WHERE n.Descripcion = '{restaurante}'
                AND Estado = '0.0'
                AND DATE(s.Fecha) >= '2024-01-01'
                AND DATE(s.Fecha) <= CURRENT_DATE()
            GROUP BY n.CodigoNegocio, n.Descripcion, anio, semana, DATE(s.Fecha)
        )
        SELECT 
            anio,
            semana,
            SUM(total_ventas) AS total_ventas
        FROM ventas
        GROUP BY anio, semana
        ORDER BY anio, semana
    '''
    df = bigquery_client.query(query).to_dataframe()
    return df

def obtener_ventas_semanales_anuales_v2(restaurante):
    """Versión alternativa usando ISOWEEK y mejor manejo de tipos de datos"""
    query = f'''
        WITH ventas AS (
            SELECT
                n.CodigoNegocio,
                n.Descripcion AS nombre_restaurante,
                EXTRACT(YEAR FROM DATE(s.Fecha)) AS anio,
                EXTRACT(ISOWEEK FROM DATE(s.Fecha)) AS semana,
                DATE(s.Fecha) AS fecha,
                CAST(s.Monto AS FLOAT64) AS monto,
                CAST(s.Estado AS STRING) AS estado_str
            FROM `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_VENTAS_ID}` s
            JOIN `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_NEGOCIOS_ID}` n
                ON s.CodigoNegocio = n.CodigoNegocio
            WHERE n.Descripcion = '{restaurante}'
                AND CAST(s.Estado AS STRING) = '0.0'
                AND DATE(s.Fecha) >= CURRENT_DATE() - INTERVAL 1 YEAR
                AND DATE(s.Fecha) <= CURRENT_DATE()
                AND s.Monto IS NOT NULL
                AND CAST(s.Monto AS STRING) != ''
        )
        SELECT 
            anio,
            semana,
            COUNT(*) as num_transacciones,
            SUM(monto) AS total_ventas,
            MIN(fecha) as fecha_inicio_semana,
            MAX(fecha) as fecha_fin_semana
        FROM ventas
        WHERE monto > 0
        GROUP BY anio, semana
        HAVING SUM(monto) > 0
        ORDER BY anio, semana
    '''
    df = bigquery_client.query(query).to_dataframe()
    return df

def generar_reporte_evolucion_anual(restaurante):
    df = obtener_ventas_semanales_anuales(restaurante)
    if df.empty:
        print(f"No hay datos para {restaurante}")
        return
    
    print(f"Datos obtenidos para {restaurante}:")
    print(df.head(10))
    print(f"Años disponibles: {sorted(df['anio'].unique())}")
    print(f"Total de registros: {len(df)}")
    
    anio_actual = datetime.now().year
    anio_anterior = anio_actual - 1
    
    # Pivotear para tener columnas por año
    df_pivot = df.pivot(index='semana', columns='anio', values='total_ventas').fillna(0)
    df_pivot = df_pivot.sort_index()
    
    # Crear un índice completo de semanas (1 a 52)
    semana_actual = datetime.now().isocalendar()[1]
    
    # Para 2024 (año completo) y 2025 (hasta semana actual)
    semanas_2024 = list(range(1, 53))  # Todas las semanas de 2024
    semanas_2025 = list(range(1, semana_actual + 1))  # Hasta la semana actual de 2025
    
    # Crear DataFrames separados para cada año
    df_2024 = df[df['anio'] == 2024].set_index('semana').reindex(semanas_2024, fill_value=0)
    df_2025 = df[df['anio'] == 2025].set_index('semana').reindex(semanas_2025, fill_value=0)
    
    # Calcular acumulados
    if not df_2024.empty:
        acum_2024 = df_2024['total_ventas'].cumsum()
    else:
        acum_2024 = pd.Series([0]*len(semanas_2024), index=semanas_2024)
    
    if not df_2025.empty:
        acum_2025 = df_2025['total_ventas'].cumsum()
    else:
        acum_2025 = pd.Series([0]*len(semanas_2025), index=semanas_2025)
    
    # Crear gráfico combinado mejorado
    fig, ax1 = plt.subplots(figsize=(16, 8))
    
    # Eje primario: Barras para ventas semanales 2025
    bars = ax1.bar(df_2025.index, df_2025['total_ventas'], 
                   color='#90caf9', alpha=0.7, label='Ventas Semanales 2025', width=0.8)
    
    ax1.set_xlabel('Semana del Año', fontsize=12)
    ax1.set_ylabel('Ventas Semanales (S/)', fontsize=12, color='#1976d2')
    ax1.tick_params(axis='y', labelcolor='#1976d2')
    ax1.set_title('Evolución Anual de Ventas - Comparación 2024 vs 2025', fontsize=16, fontweight='bold', pad=20)
    
    # Eje secundario: Líneas para acumulados
    ax2 = ax1.twinx()
    
    # Línea YTD 2025 (azul oscuro, sólida)
    line1 = ax2.plot(acum_2025.index, acum_2025.values, 
                     color='#1976d2', linewidth=3, marker='o', markersize=4,
                     label='YTD 2025', linestyle='-')
    
    # Línea YTD 2024 (gris, discontinua) - solo hasta la semana actual para comparar
    acum_2024_comparable = acum_2024.iloc[:len(acum_2025)]
    line2 = ax2.plot(acum_2024_comparable.index, acum_2024_comparable.values,
                     color='#757575', linewidth=3, marker='s', markersize=4,
                     label='YTD 2024', linestyle='--')
    
    ax2.set_ylabel('Ventas Acumuladas (S/)', fontsize=12, color='#757575')
    ax2.tick_params(axis='y', labelcolor='#757575')
    
    # Configurar leyenda combinada
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=10)
    
    # Mejorar formato del gráfico
    ax1.grid(True, alpha=0.3)
    ax1.set_xlim(0.5, len(df_2025) + 0.5)
    
    # Agregar anotaciones de tendencia
    if len(acum_2025) > 1 and len(acum_2024_comparable) > 1:
        diferencia_final = acum_2025.iloc[-1] - acum_2024_comparable.iloc[-1]
        porcentaje_cambio = (diferencia_final / acum_2024_comparable.iloc[-1] * 100) if acum_2024_comparable.iloc[-1] != 0 else 0
        
        # Texto de tendencia
        tendencia_text = f"Tendencia: {porcentaje_cambio:+.1f}% vs 2024"
        ax1.text(0.02, 0.95, tendencia_text, transform=ax1.transAxes, 
                bbox=dict(boxstyle="round,pad=0.3", facecolor="lightblue", alpha=0.7),
                fontsize=11, fontweight='bold')
    
    plt.tight_layout()
    
    # Guardar gráfico
    os.makedirs('reportes3', exist_ok=True)
    plt.savefig(f'reportes3/evolucion_anual_{restaurante.replace(" ", "_").lower()}.png', 
                dpi=300, bbox_inches='tight')
    plt.close()
    
    # Crear tabla de análisis detallado
    tabla_analisis = pd.DataFrame({
        'Semana': range(1, len(acum_2025) + 1),
        'Ventas_2025': df_2025['total_ventas'].iloc[:len(acum_2025)].values,
        'Ventas_2024': df_2024['total_ventas'].iloc[:len(acum_2025)].values,
        'Acumulado_2025': acum_2025.values,
        'Acumulado_2024': acum_2024_comparable.values,
        'Diferencia_Semanal': df_2025['total_ventas'].iloc[:len(acum_2025)].values - df_2024['total_ventas'].iloc[:len(acum_2025)].values,
        'Diferencia_Acumulada': acum_2025.values - acum_2024_comparable.values
    })
    
    # Calcular porcentajes de cambio
    tabla_analisis['Cambio_Semanal_%'] = (tabla_analisis['Diferencia_Semanal'] / tabla_analisis['Ventas_2024'] * 100).replace([float('inf'), -float('inf')], 0).fillna(0)
    tabla_analisis['Cambio_Acumulado_%'] = (tabla_analisis['Diferencia_Acumulada'] / tabla_analisis['Acumulado_2024'] * 100).replace([float('inf'), -float('inf')], 0).fillna(0)
    
    # Guardar tabla
    tabla_analisis.to_csv(f'reportes3/tabla_evolucion_anual_{restaurante.replace(" ", "_").lower()}.csv', index=False)
    
    print(f'\nReporte de evolución anual generado para {restaurante}')
    print(f'Ventas acumuladas 2025: S/ {acum_2025.iloc[-1]:,.2f}')
    print(f'Ventas acumuladas 2024: S/ {acum_2024_comparable.iloc[-1]:,.2f}')
    print(f'Diferencia: S/ {acum_2025.iloc[-1] - acum_2024_comparable.iloc[-1]:,.2f}')
    
    # Mostrar resumen de tendencias    print(f'\nAnálisis de tendencias semanales:')
    semanas_positivas = len(tabla_analisis[tabla_analisis['Cambio_Semanal_%'] > 0])
    semanas_negativas = len(tabla_analisis[tabla_analisis['Cambio_Semanal_%'] < 0])
    print(f'Semanas con crecimiento: {semanas_positivas}/{len(tabla_analisis)}')
    print(f'Semanas con decrecimiento: {semanas_negativas}/{len(tabla_analisis)}')
    
    # Generar gráfico adicional de tendencias
    generar_grafico_tendencias(tabla_analisis, restaurante)
    
    return tabla_analisis


def generar_grafico_tendencias(tabla_analisis, restaurante):
    """Genera gráfico específico de tendencias semanales comparativas"""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10))
    
    # Gráfico 1: Comparación de ventas semanales
    ax1.plot(tabla_analisis['Semana'], tabla_analisis['Ventas_2025'], 
             color='#1976d2', linewidth=2, marker='o', label='2025', markersize=5)
    ax1.plot(tabla_analisis['Semana'], tabla_analisis['Ventas_2024'], 
             color='#757575', linewidth=2, marker='s', label='2024', markersize=5, linestyle='--')
    
    ax1.set_title(f'Tendencia de Ventas Semanales - {restaurante}', fontsize=14, fontweight='bold')
    ax1.set_xlabel('Semana del Año')
    ax1.set_ylabel('Ventas Semanales (S/)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Resaltar semanas con mejor/peor performance
    mejor_semana = tabla_analisis.loc[tabla_analisis['Cambio_Semanal_%'].idxmax()]
    peor_semana = tabla_analisis.loc[tabla_analisis['Cambio_Semanal_%'].idxmin()]
    
    ax1.annotate(f'Mejor: Semana {int(mejor_semana["Semana"])}\n+{mejor_semana["Cambio_Semanal_%"]:.1f}%', 
                xy=(mejor_semana['Semana'], mejor_semana['Ventas_2025']),
                xytext=(10, 10), textcoords='offset points',
                bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgreen", alpha=0.7),
                fontsize=9)
    
    # Gráfico 2: Porcentaje de cambio semanal
    colors = ['green' if x > 0 else 'red' for x in tabla_analisis['Cambio_Semanal_%']]
    bars = ax2.bar(tabla_analisis['Semana'], tabla_analisis['Cambio_Semanal_%'], 
                   color=colors, alpha=0.7, width=0.8)
    
    ax2.axhline(y=0, color='black', linestyle='-', linewidth=0.8)
    ax2.set_title('Cambio Porcentual Semanal vs 2024', fontsize=14, fontweight='bold')
    ax2.set_xlabel('Semana del Año')
    ax2.set_ylabel('Cambio Porcentual (%)')
    ax2.grid(True, alpha=0.3)
      # Agregar línea de tendencia
    z = np.polyfit(tabla_analisis['Semana'], tabla_analisis['Cambio_Semanal_%'], 1)
    p = np.poly1d(z)
    ax2.plot(tabla_analisis['Semana'], p(tabla_analisis['Semana']), 
             "r--", alpha=0.8, linewidth=2, label=f'Tendencia: {z[0]:.2f}%/semana')
    ax2.legend()
    
    plt.tight_layout()
    plt.savefig(f'reportes3/tendencias_{restaurante.replace(" ", "_").lower()}.png', 
                dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f'Gráfico de tendencias generado: tendencias_{restaurante.replace(" ", "_").lower()}.png')


        
if __name__ == "__main__":
    generar_reporte_evolucion_anual(RESTAURANTE_OBJETIVO)

