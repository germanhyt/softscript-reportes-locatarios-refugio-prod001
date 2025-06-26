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
                    WHEN EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = 2024 THEN
                        FLOOR((DATE_DIFF(DATE(s.FechaIntegrada), DATE('2024-01-01'), DAY) + 
                               EXTRACT(DAYOFWEEK FROM DATE('2024-01-01')) - 2) / 7) + 1
                    WHEN EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = 2025 THEN
                        FLOOR((DATE_DIFF(DATE(s.FechaIntegrada), DATE('2025-01-01'), DAY) + 
                               EXTRACT(DAYOFWEEK FROM DATE('2025-01-01')) - 2) / 7) + 1
                    ELSE 1
                END AS semana_powerbi
            FROM `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_VENTAS_ID}` s
            JOIN `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_NEGOCIOS_ID}` n
                ON s.CodigoNegocio = n.CodigoNegocio
            WHERE n.Descripcion = '{restaurante}'
                AND Estado = '0.0'
                AND DATE(s.FechaIntegrada) >= '2024-01-01'
                AND DATE(s.FechaIntegrada) <= CURRENT_DATE()
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
    """Genera reporte comparativo con datos 2025 reales vs 2024 REALES y tabla completa"""
    
    # Obtener datos reales del 2025
    df = obtener_ventas_semanales_powerbi_compatible(restaurante)
    if df.empty:
        print(f"No hay datos para {restaurante}")
        return
    
    df_2025 = df[df['anio'] == 2025].copy()
    if df_2025.empty:
        print("No hay datos del 2025")
        return    # *** USAR DATOS REALES DEL 2024 ***
    df_2024_real = obtener_datos_2024_referencia_real(restaurante)
    
    # Preparar datos para comparación - MOSTRAR TODAS LAS SEMANAS DESDE LA 1
    semana_actual = datetime.now().isocalendar()[1]
    
    print(f"Generando reporte comparativo para {restaurante}:")
    print(f"- Datos 2025 (reales): {len(df_2025)} semanas")
    if not df_2024_real.empty:
        print(f"- Datos 2024 (reales): {len(df_2024_real)} semanas disponibles")
        print(f"- Comparación justa: Ambos años hasta semana {semana_actual}")
    # else:
    #     print("- Datos 2024: No disponibles, usando datos simulados")
    
    # Preparar datos para comparación - MOSTRAR TODAS LAS SEMANAS DESDE LA 1
    semanas_completas = list(range(1, semana_actual + 1))
    
    # Series para ambos años
    ventas_2025 = pd.Series(index=semanas_completas, dtype=float).fillna(0)
    ventas_2024 = pd.Series(index=semanas_completas, dtype=float).fillna(0)
      # Llenar con datos reales 2025
    for _, row in df_2025.iterrows():
        if int(row['semana']) in ventas_2025.index:
            ventas_2025[int(row['semana'])] = row['total_ventas']
    
    # Llenar con datos REALES 2024 - SOLO HASTA LA SEMANA ACTUAL PARA COMPARACIÓN JUSTA
    for _, row in df_2024_real.iterrows():
        semana_2024 = int(row['semana'])
        # Solo incluir semanas hasta la semana actual para comparación justa
        if semana_2024 in ventas_2024.index and semana_2024 <= semana_actual:
            ventas_2024[semana_2024] = row['total_ventas']
    
    # Calcular acumulados
    acum_2025 = ventas_2025.cumsum()
    acum_2024 = ventas_2024.cumsum()
    
    # Crear tabla de comparación con las columnas solicitadas
    tabla_comparativa = pd.DataFrame({
        'nro_semana': semanas_completas,
        'ventas_actuales': ventas_2025.values,
        'ventas_año_anterior': ventas_2024.values,
        'diferencia': ventas_2025.values - ventas_2024.values,
        'diferencia_porcentual': ((ventas_2025.values - ventas_2024.values) / ventas_2024.values * 100)
    })
     
    # Manejar divisiones por cero
    tabla_comparativa['diferencia_porcentual'] = tabla_comparativa['diferencia_porcentual'].replace([float('inf'), -float('inf')], 0).fillna(0)
    
    # Agregar acumulados
    tabla_comparativa['acum_actuales'] = acum_2025.values
    tabla_comparativa['acum_año_anterior'] = acum_2024.values
    tabla_comparativa['dif_acum'] = acum_2025.values - acum_2024.values
    tabla_comparativa['dif_acum_porcentual'] = ((acum_2025.values - acum_2024.values) / acum_2024.values * 100)
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
                        label='Ventas 2025 (Actuales)', color='#4CAF50', alpha=0.8)
    bars2 = ax_main.bar(x + width/2, tabla_comparativa['ventas_año_anterior'], width,
                        label='Ventas 2024 (Referencia)', color='#FFC107', alpha=0.8)
    
    # Eje secundario para acumulados
    ax_secondary = ax_main.twinx()
    ax_secondary.plot(x, tabla_comparativa['acum_actuales'], 
                     color='#2E7D32', linewidth=3, marker='o', markersize=4,
                     label='YTD 2025 (acumulado)', linestyle='-')
    ax_secondary.plot(x, tabla_comparativa['acum_año_anterior'],
                     color='#F57C00', linewidth=3, marker='s', markersize=4,
                     label='YTD 2024 (acumulado)', linestyle='--')
    
    # Configurar ejes
    ax_main.set_xlabel('Semana del Año', fontsize=12)
    ax_main.set_ylabel('Ventas Semanales (S/)', fontsize=12)
    ax_secondary.set_ylabel('Ventas Acumuladas (S/)', fontsize=12)
    ax_main.set_title('Evolución Anual de Ventas - Tendencia Comparativa 2024 vs 2025', 
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
    total_2025 = tabla_comparativa['acum_actuales'].iloc[-1]
    total_2024 = tabla_comparativa['acum_año_anterior'].iloc[-1]
    diferencia_total = total_2025 - total_2024
    porcentaje_total = (diferencia_total / total_2024 * 100) if total_2024 != 0 else 0
    
    # Texto de estadísticas en el gráfico
    stats_text = f"Total 2025: S/ {total_2025:,.0f} | Total 2024: S/ {total_2024:,.0f} | Diferencia: {porcentaje_total:+.1f}%"
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
                 f'Total 2025: S/ {total_2025:,.0f} | Total 2024: S/ {total_2024:,.0f} | '
                 f'Diferencia: {porcentaje_total:+.1f}%', 
                 fontsize=16, fontweight='bold', y=0.98)
    
    # Guardar archivos
    os.makedirs('reportes3', exist_ok=True)
    plt.savefig(f'reportes3/reporte_comparativo_completo_{restaurante.replace(" ", "_").lower()}.png', 
                dpi=300, bbox_inches='tight')
    plt.close()
      
    # Guardar tabla completa en CSV
    tabla_comparativa = []
    # tabla_comparativa.to_csv(f'reportes3/tabla_comparativa_completa_{restaurante.replace(" ", "_").lower()}.csv', index=False)
    
    print(f'\n✅ Reporte comparativo completo generado para {restaurante}')
    print(f'📊 Archivos creados:')
    print(f'   - reporte_comparativo_completo_{restaurante.replace(" ", "_").lower()}.png')
    print(f'   - tabla_comparativa_completa_{restaurante.replace(" ", "_").lower()}.csv')
    print(f'\n📈 Resumen de resultados:')
    print(f'   - Total ventas 2025: S/ {total_2025:,.0f}')
    print(f'   - Total ventas 2024: S/ {total_2024:,.0f}')
    print(f'   - Diferencia: S/ {diferencia_total:,.0f} ({porcentaje_total:+.1f}%)')
    
    return tabla_comparativa


def buscar_datos_reales_2024_todos_restaurantes():
    """Busca datos reales del 2024 en todos los restaurantes para usar como referencia"""
    query = f'''
        WITH ventas_2024 AS (
            SELECT
                n.Descripcion AS nombre_restaurante,
                EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) AS anio,
                -- Calcular semana compatible con Power BI WEEKNUM(fecha,11)
                CASE 
                    WHEN EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = 2024 THEN
                        FLOOR((DATE_DIFF(DATE(s.FechaIntegrada), DATE('2024-01-01'), DAY) + 
                               EXTRACT(DAYOFWEEK FROM DATE('2024-01-01')) - 2) / 7) + 1
                    ELSE 1
                END AS semana_powerbi,
                DATE(s.FechaIntegrada) AS fecha,
                CAST(s.Monto AS FLOAT64) AS monto
            FROM `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_VENTAS_ID}` s
            JOIN `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_NEGOCIOS_ID}` n
                ON s.CodigoNegocio = n.CodigoNegocio
            WHERE Estado = '0.0'
                AND EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = 2024
                AND DATE(s.FechaIntegrada) >= '2024-01-01'
                AND DATE(s.FechaIntegrada) <= '2024-12-31'
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
        FROM ventas_2024
        WHERE semana_powerbi BETWEEN 1 AND 53
        GROUP BY nombre_restaurante, semana_powerbi
        HAVING SUM(monto) > 0
        ORDER BY nombre_restaurante, semana_powerbi
    '''
    df = bigquery_client.query(query).to_dataframe()
    return df

def obtener_datos_2024_referencia_real(restaurante_objetivo):
    """Obtiene datos reales del 2024 del restaurante objetivo o de restaurantes similares"""
    
    # Primero intentar obtener datos del restaurante objetivo
    query_objetivo = f'''
        WITH ventas_2024 AS (
            SELECT
                n.Descripcion AS nombre_restaurante,
                EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) AS anio,
                CASE 
                    WHEN EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = 2024 THEN
                        FLOOR((DATE_DIFF(DATE(s.FechaIntegrada), DATE('2024-01-01'), DAY) + 
                               EXTRACT(DAYOFWEEK FROM DATE('2024-01-01')) - 2) / 7) + 1
                    ELSE 1
                END AS semana_powerbi,
                CAST(s.Monto AS FLOAT64) AS monto
            FROM `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_VENTAS_ID}` s
            JOIN `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_NEGOCIOS_ID}` n
                ON s.CodigoNegocio = n.CodigoNegocio
            WHERE n.Descripcion = '{restaurante_objetivo}'
                AND Estado = '0.0'
                AND EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = 2024
                AND s.Monto IS NOT NULL
                AND CAST(s.Monto AS FLOAT64) > 0
        )
        SELECT 
            2024 as anio,
            semana_powerbi as semana,
            COUNT(*) as num_transacciones,
            SUM(monto) AS total_ventas
        FROM ventas_2024
        WHERE semana_powerbi BETWEEN 1 AND 53
        GROUP BY semana_powerbi
        HAVING SUM(monto) > 0
        ORDER BY semana_powerbi
    '''
    
    df_objetivo = bigquery_client.query(query_objetivo).to_dataframe()
    
    if not df_objetivo.empty:
        print(f"✅ Encontrados datos reales del 2024 para {restaurante_objetivo}: {len(df_objetivo)} semanas")
        return df_objetivo
    
    # Si no hay datos del restaurante objetivo, buscar datos de otros restaurantes como referencia
    print(f"⚠️ No se encontraron datos del 2024 para {restaurante_objetivo}")
    print("🔍 Buscando datos del 2024 en otros restaurantes como referencia...")
    
    df_todos = buscar_datos_reales_2024_todos_restaurantes()
    
    if not df_todos.empty:
        # Usar el restaurante con más datos del 2024
        restaurante_con_mas_datos = df_todos.groupby('nombre_restaurante')['total_ventas'].sum().idxmax()
        df_referencia = df_todos[df_todos['nombre_restaurante'] == restaurante_con_mas_datos].copy()
        df_referencia = df_referencia[['semana', 'total_ventas', 'num_transacciones']].copy()
        df_referencia['anio'] = 2024
        
        print(f"📊 Usando datos del 2024 de '{restaurante_con_mas_datos}' como referencia: {len(df_referencia)} semanas")
        return df_referencia
    
    print("❌ No se encontraron datos del 2024 en ningún restaurante")
    return pd.DataFrame()
    
if __name__ == "__main__":
    print("=== GENERADOR DE REPORTES COMPARATIVOS ===")
    print(f"Restaurante objetivo: {RESTAURANTE_OBJETIVO}")
    print("\nGenerando reporte comparativo completo...")
    print("- Datos 2025: Reales de la base de datos (compatible con Power BI)")
    print("- Datos 2024: Simulados para comparación")
    print("- Tabla integrada con: nro_semana, ventas_actuales, ventas_año_anterior, diferencia, diferencia_porcentual")
    
    generar_reporte_comparativo_con_tabla(RESTAURANTE_OBJETIVO)

