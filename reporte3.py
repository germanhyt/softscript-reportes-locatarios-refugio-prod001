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

# Locatarios:
locatarios_map = [
    'Quién pidió pollo',
    'Puerto Mancora',   
    'Patio Cavenecia',
    'Caja China Criolla',
    'Bros',
    'Belgofre',
    'Limanesas',
    'Saltao',
    'Vikinga',
    'Bodega Turca',
    'La 22',    
    'Taqueado',
    'Choza de la Anaconda',
    'MR SMASH',
    'Sisa Cafe',
    'Hanzo',
    'La Victoria',
    'Curich',
    'Anticuching',
    'Bar Refugio'
]


# BigQuery Configuration
PROJECT_ID = 'neat-chain-450900-a1'
DATASET_VENTAS_ID = 'Ventas'
# LOCAL
SERVICE_ACCOUNT_FILE = r'C:\Users\gcbso\Downloads\EMPRESA GCB\REFUGIO PROYECTO PYTHON LOCAL\neat-chain-450900-a1-f67bb083925b.json'
# N8N
# SERVICE_ACCOUNT_FILE = '/data/scripts/neat-chain-450900-a1-f67bb083925b.json'

# LOCAL PATH SAVE REPORTS
OUTPUT_FOLDER = 'reportes3'
# N8N PATH SAVE REPORTS
# OUTPUT_FOLDER = '/data/scripts/reportes3'

TABLE_VENTAS_ID = 'sales_df'
TABLE_CATEGORIAS_ID = 'categorias'
TABLE_METAS_ID = 'MontosMeta'
TABLE_NEGOCIOS_ID = 'Negocios'


# BigQuery client
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
bigquery_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)


# Función para verfiicar si la tabla sales_df existe
# def visualizar_tabla_sales_df():
#     query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_VENTAS_ID}.sales_df` LIMIT 100"
#     try:
#         df = bigquery_client.query(query).to_dataframe()
#         print("Tabla 'sales_df':")
#         print(df.head())  # Mostrar las primeras filas
#     except Exception as e:
#         print(f"Error al consultar la tabla 'sales_df': {e}")

# === CONFIGURACIÓN DINÁMICA DE AÑOS ===
ANIO_ACTUAL = datetime.now().year          
ANIO_COMPARACION = ANIO_ACTUAL - 1     


# RESTAURANTE_OBJETIVO = 'Anticuching'  

# Función para cambiar años dinámicamente
def configurar_anos_comparacion(anio_actual=None, anio_comparacion=None):
    """
    Permite cambiar dinámicamente los años de comparación
    Args:
        anio_actual: Año principal para el análisis (por defecto 2025)
        anio_comparacion: Año para comparar (por defecto se calcula automáticamente como año_actual - 1)
    
    Ejemplos de uso:
        configurar_anos_comparacion()  # Usa defaults: 2025 vs 2024
        configurar_anos_comparacion(anio_comparacion=2023)  # 2025 vs 2023
        configurar_anos_comparacion(anio_actual=2024, anio_comparacion=2022)  # 2024 vs 2022
    """
    global ANIO_ACTUAL, ANIO_COMPARACION
    
    if anio_actual:
        ANIO_ACTUAL = anio_actual
    
    if anio_comparacion:
        ANIO_COMPARACION = anio_comparacion
    else:
        # Si no se especifica año de comparación, usar el anterior al año actual
        ANIO_COMPARACION = ANIO_ACTUAL - 1
    
    print(f"📅 Configuración actualizada:")
    print(f"   - Año actual: {ANIO_ACTUAL}")
    print(f"   - Año comparación: {ANIO_COMPARACION}")



def obtener_ventas_semanales_powerbi_compatible(restaurante):
    """Función alternativa que calcula las semanas exactamente igual que Power BI WEEKNUM(fecha,11)"""
    query = f'''
        WITH ventas_raw AS (
            SELECT
                n.CodigoNegocio,
                n.Descripcion AS nombre_restaurante,
                EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) AS anio,
                DATE(s.FechaIntegrada) AS fecha,
                CAST(s.Monto AS FLOAT64) AS monto,
                -- Calcular el número de semana manualmente como Power BI WEEKNUM(fecha,11)
                -- Sistema 11: Lunes es el primer día de la semana, primera semana contiene 1 de enero
                CASE 
                    WHEN EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = {ANIO_ACTUAL} THEN
                        FLOOR((DATE_DIFF(DATE(s.FechaIntegrada), DATE('{ANIO_ACTUAL}-01-01'), DAY) + 
                               EXTRACT(DAYOFWEEK FROM DATE('{ANIO_ACTUAL}-01-01')) - 2) / 7) + 1
                    WHEN EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = {ANIO_COMPARACION} THEN
                        FLOOR((DATE_DIFF(DATE(s.FechaIntegrada), DATE('{ANIO_COMPARACION}-01-01'), DAY) + 
                               EXTRACT(DAYOFWEEK FROM DATE('{ANIO_COMPARACION}-01-01')) - 2) / 7) + 1
                    ELSE 1
                END AS semana_powerbi
            FROM `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_VENTAS_ID}` s
            JOIN `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_NEGOCIOS_ID}` n
                ON s.CodigoNegocio = n.CodigoNegocio
            WHERE n.Descripcion = '{restaurante}'
                AND Estado = '0.0'
                AND (DATE(s.FechaIntegrada) >= '{ANIO_COMPARACION}-01-01' 
                     AND DATE(s.FechaIntegrada) <= CURRENT_DATE())
                AND s.Monto IS NOT NULL
                AND CAST(s.Monto AS FLOAT64) > 0
        )
        SELECT 
            anio,
            semana_powerbi as semana,
            COUNT(*) as num_transacciones,
            SUM(monto) AS total_ventas,
            MIN(fecha) as fecha_inicio_semana,
            MAX(fecha) as fecha_fin_semana
        FROM ventas_raw
        WHERE semana_powerbi BETWEEN 1 AND 53
        GROUP BY anio, semana_powerbi
        HAVING SUM(monto) > 0
        ORDER BY anio, semana_powerbi
    '''
    df = bigquery_client.query(query).to_dataframe()
    return df


def generar_reporte_comparativo_con_tabla(restaurante):
    """Genera reporte comparativo dinámico usando las variables globales de años"""
    
    # Obtener datos reales usando años dinámicos
    df = obtener_ventas_semanales_powerbi_compatible(restaurante)
    if df.empty:
        print(f"No hay datos para {restaurante}")
        return
    
    df_actual = df[df['anio'] == ANIO_ACTUAL].copy()
    if df_actual.empty:
        print(f"No hay datos del {ANIO_ACTUAL}")
        return
      # *** USAR DATOS REALES DEL AÑO DE COMPARACIÓN ***
    df_comparacion_real = obtener_datos_anio_comparacion_real(restaurante)
    
    # Preparar datos para comparación - MOSTRAR TODAS LAS SEMANAS DESDE LA 1
    semana_actual = datetime.now().isocalendar()[1]
    
    print(f"Generando reporte comparativo para {restaurante}:")
    print(f"- Datos {ANIO_ACTUAL} (reales): {len(df_actual)} semanas")
    if not df_comparacion_real.empty:
        print(f"- Datos {ANIO_COMPARACION} (reales): {len(df_comparacion_real)} semanas disponibles")
        print(f"- Comparación justa: Ambos años hasta semana {semana_actual}")
    
    # Preparar datos para comparación - MOSTRAR TODAS LAS SEMANAS DESDE LA 1
    semanas_completas = list(range(1, semana_actual + 1))
    
    # Series para ambos años
    ventas_actual = pd.Series(index=semanas_completas, dtype=float).fillna(0)
    ventas_comparacion = pd.Series(index=semanas_completas, dtype=float).fillna(0)
    
    # Llenar con datos reales del año actual
    for _, row in df_actual.iterrows():
        if int(row['semana']) in ventas_actual.index:
            ventas_actual[int(row['semana'])] = row['total_ventas']
    
    # Llenar con datos REALES del año de comparación - SOLO HASTA LA SEMANA ACTUAL
    for _, row in df_comparacion_real.iterrows():
        semana_comp = int(row['semana'])
        # Solo incluir semanas hasta la semana actual para comparación justa
        if semana_comp in ventas_comparacion.index and semana_comp <= semana_actual:
            ventas_comparacion[semana_comp] = row['total_ventas']
    
    # Calcular acumulados
    acum_actual = ventas_actual.cumsum()
    acum_comparacion = ventas_comparacion.cumsum()
      # Crear tabla de comparación con las columnas solicitadas
    tabla_comparativa = pd.DataFrame({
        'nro_semana': semanas_completas,
        'ventas_actuales': ventas_actual.values,
        'ventas_año_anterior': ventas_comparacion.values,
        'diferencia': ventas_actual.values - ventas_comparacion.values,
        'diferencia_porcentual': ((ventas_actual.values - ventas_comparacion.values) / ventas_comparacion.values * 100)
    })
     
    # Manejar divisiones por cero
    tabla_comparativa['diferencia_porcentual'] = tabla_comparativa['diferencia_porcentual'].replace([float('inf'), -float('inf')], 0).fillna(0)
      # Agregar acumulados
    tabla_comparativa['acum_actuales'] = acum_actual.values
    tabla_comparativa['acum_año_anterior'] = acum_comparacion.values
    tabla_comparativa['dif_acum'] = acum_actual.values - acum_comparacion.values
    tabla_comparativa['dif_acum_porcentual'] = ((acum_actual.values - acum_comparacion.values) / acum_comparacion.values * 100)
    tabla_comparativa['dif_acum_porcentual'] = tabla_comparativa['dif_acum_porcentual'].replace([float('inf'), -float('inf')], 0).fillna(0)
    
    # Crear figura con gráfico y tabla integrada - TAMAÑO AUMENTADO
    fig = plt.figure(figsize=(24, 20))
    
    # Layout: 50% gráfico, 50% tabla (aprovechar espacio)
    gs = fig.add_gridspec(2, 1, height_ratios=[1, 1], hspace=0.15)
    
    # === GRÁFICO PRINCIPAL ===
    ax_main = fig.add_subplot(gs[0])
    
    # Gráfico de barras comparativo
    x = np.arange(len(semanas_completas))
    width = 0.35
    
    bars1 = ax_main.bar(x - width/2, tabla_comparativa['ventas_actuales'], width,
                        label=f'Ventas {ANIO_ACTUAL} (Actuales)', color='#4CAF50', alpha=0.8)
    bars2 = ax_main.bar(x + width/2, tabla_comparativa['ventas_año_anterior'], width,
                        label=f'Ventas {ANIO_COMPARACION} (Referencia)', color='#FFC107', alpha=0.8)
    
    # Eje secundario para acumulados
    ax_secondary = ax_main.twinx()
    ax_secondary.plot(x, tabla_comparativa['acum_actuales'], 
                     color='#2E7D32', linewidth=3, marker='o', markersize=4,
                     label=f'YTD {ANIO_ACTUAL} (acumulado)', linestyle='-')
    ax_secondary.plot(x, tabla_comparativa['acum_año_anterior'],
                     color='#F57C00', linewidth=3, marker='s', markersize=4,
                     label=f'YTD {ANIO_COMPARACION} (acumulado)', linestyle='--')
    
    # Configurar ejes
    ax_main.set_xlabel('Semana del Año', fontsize=12)
    ax_main.set_ylabel('Ventas Semanales (S/)', fontsize=12)
    ax_secondary.set_ylabel('Ventas Acumuladas (S/)', fontsize=12)
    ax_main.set_title(f'Evolución Anual de Ventas - Tendencia Comparativa {ANIO_COMPARACION} vs {ANIO_ACTUAL}', 
                      fontsize=16, fontweight='bold', pad=20)
      # Etiquetas del eje X - MOSTRAR TODAS LAS SEMANAS
    ax_main.set_xticks(x)  # Mostrar todas las semanas
    ax_main.set_xticklabels([f'S{i:02d}' for i in semanas_completas], rotation=45, fontsize=8)
    
    # Leyenda combinada
    lines1, labels1 = ax_main.get_legend_handles_labels()
    lines2, labels2 = ax_secondary.get_legend_handles_labels()
    ax_main.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=10)
    
    ax_main.grid(True, alpha=0.3)
      # === ESTADÍSTICAS RESUMIDAS ===
    total_actual = tabla_comparativa['acum_actuales'].iloc[-1]
    total_comparacion = tabla_comparativa['acum_año_anterior'].iloc[-1]
    diferencia_total = total_actual - total_comparacion
    porcentaje_total = (diferencia_total / total_comparacion * 100) if total_comparacion != 0 else 0
    
    # Texto de estadísticas en el gráfico
    stats_text = f"Total {ANIO_ACTUAL}: S/ {total_actual:,.0f} | Total {ANIO_COMPARACION}: S/ {total_comparacion:,.0f} | Diferencia: {porcentaje_total:+.1f}%"
    ax_main.text(0.5, 0.95, stats_text, transform=ax_main.transAxes, 
                bbox=dict(boxstyle="round,pad=0.3", facecolor="lightblue", alpha=0.8),
                fontsize=12, fontweight='bold', ha='center')
      # === TABLA DE DATOS ===
    ax_table = fig.add_subplot(gs[1])
    ax_table.axis('off')
      # Preparar datos de la tabla - MOSTRAR TODAS LAS SEMANAS DESDE LA 1
    tabla_display = tabla_comparativa.copy().round(0)  # TODAS las semanas, no solo las últimas
    
    tabla_formateada = []
    for _, row in tabla_display.iterrows():
        tabla_formateada.append([
            f"S{int(row['nro_semana']):02d}",
            f"S/ {row['ventas_actuales']:,.0f}",
            f"S/ {row['ventas_año_anterior']:,.0f}",
            f"S/ {row['diferencia']:+,.0f}",
            f"{row['diferencia_porcentual']:+.1f}%",
            f"S/ {row['acum_actuales']:,.0f}",
            f"S/ {row['acum_año_anterior']:,.0f}",
            f"S/ {row['dif_acum']:+,.0f}",
            f"{row['dif_acum_porcentual']:+.1f}%"
        ])
    
    # Crear tabla con las columnas solicitadas
    headers = ['Nro\nSemana', 'Ventas\nActuales', 'Ventas Año\nAnterior', 'Diferencia', 'Diferencia\nPorcentual',
               'Acum\nActuales', 'Acum Año\nAnterior', 'Dif\nAcumulada', 'Dif Acum\nPorcentual']
    
    table = ax_table.table(cellText=tabla_formateada,
                          colLabels=headers,
                          cellLoc='center',
                          loc='center',
                          bbox=[0, 0, 1, 1])
    
    # Formatear tabla
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 2)
    
    # Colorear encabezados
    for i in range(len(headers)):
        table[(0, i)].set_facecolor('#1976D2')
        table[(0, i)].set_text_props(weight='bold', color='white')
    
    # Colorear filas y diferencias
    for i in range(1, len(tabla_formateada) + 1):
        for j in range(len(headers)):
            # Filas alternadas
            if i % 2 == 0:
                table[(i, j)].set_facecolor('#f8f9fa')
            
            # Colorear diferencias
            if j in [3, 4, 7, 8]:  # Columnas de diferencias
                try:
                    valor_dif = tabla_display.iloc[i-1]['diferencia'] if j in [3, 4] else tabla_display.iloc[i-1]['dif_acum']
                    if valor_dif > 0:
                        table[(i, j)].set_facecolor('#e8f5e8')
                        table[(i, j)].set_text_props(color='#2e7d32', weight='bold')
                    elif valor_dif < 0:
                        table[(i, j)].set_facecolor('#ffebee')
                        table[(i, j)].set_text_props(color='#d32f2f', weight='bold')
                except:
                    pass
    # Título principal
    fig.suptitle(f'Reporte Comparativo Completo - {restaurante}\n'
                 f'Total {ANIO_ACTUAL}: S/ {total_actual:,.0f} | Total {ANIO_COMPARACION}: S/ {total_comparacion:,.0f} | '
                 f'Diferencia: {porcentaje_total:+.1f}%', 
                 fontsize=16, fontweight='bold', y=0.98)
    
    # Guardar archivos
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    # plt.savefig(f'reportes3/reporte_comparativo_completo_{restaurante.replace(" ", "_").lower()}.png', 
    #             dpi=300, bbox_inches='tight')
    plt.savefig(f"{OUTPUT_FOLDER}/reporte_comparativo_completo_{restaurante.replace(' ', '_').lower()}.png", 
                dpi=300, bbox_inches='tight') 
    plt.close()
      
    # Guardar tabla completa en CSV
    tabla_comparativa = []
    # tabla_comparativa.to_csv(f'reportes3/tabla_comparativa_completa_{restaurante.replace(" ", "_").lower()}.csv', index=False)    
    print(f'\nReporte comparativo completo generado para {restaurante}')
    print(f'Archivos creados:')
    print(f'   - reporte_comparativo_completo_{restaurante.replace(" ", "_").lower()}.png')
    print(f'   - tabla_comparativa_completa_{restaurante.replace(" ", "_").lower()}.csv')
    print(f'\nResumen de resultados:')
    print(f'   - Total ventas {ANIO_ACTUAL}: S/ {total_actual:,.0f}')
    print(f'   - Total ventas {ANIO_COMPARACION}: S/ {total_comparacion:,.0f}')
    print(f'   - Diferencia: S/ {diferencia_total:,.0f} ({porcentaje_total:+.1f}%)')
    
    return tabla_comparativa


def buscar_datos_reales_anio_comparacion_todos_restaurantes():
    """Busca datos reales del año de comparación en todos los restaurantes para usar como referencia"""
    query = f'''
        WITH ventas_comparacion AS (
            SELECT
                n.Descripcion AS nombre_restaurante,
                EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) AS anio,
                -- Calcular semana compatible con Power BI WEEKNUM(fecha,11)
                CASE 
                    WHEN EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = {ANIO_COMPARACION} THEN
                        FLOOR((DATE_DIFF(DATE(s.FechaIntegrada), DATE('{ANIO_COMPARACION}-01-01'), DAY) + 
                               EXTRACT(DAYOFWEEK FROM DATE('{ANIO_COMPARACION}-01-01')) - 2) / 7) + 1
                    ELSE 1
                END AS semana_powerbi,
                DATE(s.FechaIntegrada) AS fecha,
                CAST(s.Monto AS FLOAT64) AS monto
            FROM `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_VENTAS_ID}` s
            JOIN `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_NEGOCIOS_ID}` n
                ON s.CodigoNegocio = n.CodigoNegocio
            WHERE Estado = '0.0'
                AND EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = {ANIO_COMPARACION}
                AND DATE(s.FechaIntegrada) >= '{ANIO_COMPARACION}-01-01'
                AND DATE(s.FechaIntegrada) <= '{ANIO_COMPARACION}-12-31'
                AND s.Monto IS NOT NULL
                AND CAST(s.Monto AS FLOAT64) > 0
        )
        SELECT 
            nombre_restaurante,
            semana_powerbi as semana,
            COUNT(*) as num_transacciones,
            SUM(monto) AS total_ventas,
            MIN(fecha) as fecha_inicio_semana,
            MAX(fecha) as fecha_fin_semana
        FROM ventas_comparacion
        WHERE semana_powerbi BETWEEN 1 AND 53
        GROUP BY nombre_restaurante, semana_powerbi
        HAVING SUM(monto) > 0
        ORDER BY nombre_restaurante, semana_powerbi
    '''
    df = bigquery_client.query(query).to_dataframe()
    return df

def obtener_datos_anio_comparacion_real(restaurante_objetivo):
    """Obtiene datos reales del año de comparación del restaurante objetivo o de restaurantes similares"""
    
    # Primero intentar obtener datos del restaurante objetivo
    query_objetivo = f'''
        WITH ventas_comparacion AS (
            SELECT
                n.Descripcion AS nombre_restaurante,
                EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) AS anio,
                CASE 
                    WHEN EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = {ANIO_COMPARACION} THEN
                        FLOOR((DATE_DIFF(DATE(s.FechaIntegrada), DATE('{ANIO_COMPARACION}-01-01'), DAY) + 
                               EXTRACT(DAYOFWEEK FROM DATE('{ANIO_COMPARACION}-01-01')) - 2) / 7) + 1
                    ELSE 1
                END AS semana_powerbi,
                CAST(s.Monto AS FLOAT64) AS monto
            FROM `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_VENTAS_ID}` s
            JOIN `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_NEGOCIOS_ID}` n
                ON s.CodigoNegocio = n.CodigoNegocio
            WHERE n.Descripcion = '{restaurante_objetivo}'
                AND Estado = '0.0'
                AND EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = {ANIO_COMPARACION}
                AND s.Monto IS NOT NULL
                AND CAST(s.Monto AS FLOAT64) > 0
        )
        SELECT 
            {ANIO_COMPARACION} as anio,
            semana_powerbi as semana,
            COUNT(*) as num_transacciones,
            SUM(monto) AS total_ventas
        FROM ventas_comparacion
        WHERE semana_powerbi BETWEEN 1 AND 53
        GROUP BY semana_powerbi
        HAVING SUM(monto) > 0
        ORDER BY semana_powerbi
    '''
    
    df_objetivo = bigquery_client.query(query_objetivo).to_dataframe()
    
    if not df_objetivo.empty:
        print(f"Encontrados datos reales del {ANIO_COMPARACION} para {restaurante_objetivo}: {len(df_objetivo)} semanas")
        return df_objetivo
    
    # Si no hay datos del restaurante objetivo, buscar datos de otros restaurantes como referencia
    print(f"No se encontraron datos del {ANIO_COMPARACION} para {restaurante_objetivo}")
    print(f"Buscando datos del {ANIO_COMPARACION} en otros restaurantes como referencia...")
    
    df_todos = buscar_datos_reales_anio_comparacion_todos_restaurantes()
    
    if not df_todos.empty:
        # Usar el restaurante con más datos del año de comparación
        restaurante_con_mas_datos = df_todos.groupby('nombre_restaurante')['total_ventas'].sum().idxmax()
        df_referencia = df_todos[df_todos['nombre_restaurante'] == restaurante_con_mas_datos].copy()
        df_referencia = df_referencia[['semana', 'total_ventas', 'num_transacciones']].copy()
        df_referencia['anio'] = ANIO_COMPARACION
        
        print(f"Usando datos del {ANIO_COMPARACION} de '{restaurante_con_mas_datos}' como referencia: {len(df_referencia)} semanas")
        return df_referencia
    
    print(f" No se encontraron datos del {ANIO_COMPARACION} en ningún restaurante")
    return pd.DataFrame()    
if __name__ == "__main__":
  
    # generar reporte de todos los locatarios
    for locatario in locatarios_map:
        print(f"\nGenerando reporte para: {locatario}")
        generar_reporte_comparativo_con_tabla(locatario)
        
    print("\n Todos los reportes fueron generados exitosamente.")
    

