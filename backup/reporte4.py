# # libraries
# import calendar
# from datetime import datetime, timedelta
# from collections import defaultdict
# import statistics
# from google.cloud import bigquery
# from google.oauth2 import service_account

# import os
# import pandas as pd
# import numpy as np
# import matplotlib.pyplot as plt
# import seaborn as sns

# # map de meses en inglés a español
# month_map = {
#     1: 'Enero',
#     2: 'Febrero',   
#     3: 'Marzo',
#     4: 'Abril',
#     5: 'Mayo',
#     6: 'Junio',
#     7: 'Julio',
#     8: 'Agosto',
#     9: 'Septiembre',
#     10: 'Octubre',
#     11: 'Noviembre',
#     12: 'Diciembre'
# }


# # BigQuery Configuration
# PROJECT_ID = 'neat-chain-450900-a1'
# DATASET_VENTAS_ID = 'Ventas'
# SERVICE_ACCOUNT_FILE = r'C:\Users\gcbso\Downloads\EMPRESA GCB\REFUGIO PROYECTO PYTHON LOCAL\neat-chain-450900-a1-f67bb083925b.json'

# TABLE_VENTAS_ID = 'sales_df'
# TABLE_CATEGORIAS_ID = 'categorias'
# TABLE_METAS_ID = 'MontosMeta'
# TABLE_NEGOCIOS_ID = 'Negocios'


# # BigQuery client
# credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
# bigquery_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)


# # Función para verfiicar si la tabla sales_df existe
# def visualizar_tabla_sales_df():
#     query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_VENTAS_ID}.sales_df` LIMIT 100"
#     try:
#         df = bigquery_client.query(query).to_dataframe()
#         print("Tabla 'sales_df':")
#         print(df.head())  # Mostrar las primeras filas
#     except Exception as e:
#         print(f"Error al consultar la tabla 'sales_df': {e}")


# # === CONFIGURACIÓN DINÁMICA DE AÑOS ===
# ANIO_ACTUAL = 2025          
# ANIO_COMPARACION = ANIO_ACTUAL - 1     


# RESTAURANTE_OBJETIVO = 'Anticuching'  # Restaurante a analizar

# # Función para cambiar años dinámicamente
# def configurar_anos_comparacion(anio_actual=None, anio_comparacion=None):
#     """
#     Permite cambiar dinámicamente los años de comparación
#     Args:
#         anio_actual: Año principal para el análisis (por defecto 2025)
#         anio_comparacion: Año para comparar (por defecto se calcula automáticamente como año_actual - 1)
    
#     Ejemplos de uso:
#         configurar_anos_comparacion()  # Usa defaults: 2025 vs 2024
#         configurar_anos_comparacion(anio_comparacion=2023)  # 2025 vs 2023
#         configurar_anos_comparacion(anio_actual=2024, anio_comparacion=2022)  # 2024 vs 2022
#     """
#     global ANIO_ACTUAL, ANIO_COMPARACION
    
#     if anio_actual:
#         ANIO_ACTUAL = anio_actual
    
#     if anio_comparacion:
#         ANIO_COMPARACION = anio_comparacion
#     else:
#         # Si no se especifica año de comparación, usar el anterior al año actual
#         ANIO_COMPARACION = ANIO_ACTUAL - 1
    
#     print(f"📅 Configuración actualizada:")
#     print(f"   - Año actual: {ANIO_ACTUAL}")
#     print(f"   - Año comparación: {ANIO_COMPARACION}")



# def obtener_ventas_semanales_powerbi_compatible(restaurante):
#     """Función alternativa que calcula las semanas exactamente igual que Power BI WEEKNUM(fecha,11)"""
#     query = f'''
#         WITH ventas_raw AS (
#             SELECT
#                 n.CodigoNegocio,
#                 n.Descripcion AS nombre_restaurante,
#                 EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) AS anio,
#                 DATE(s.FechaIntegrada) AS fecha,
#                 CAST(s.Monto AS FLOAT64) AS monto,
#                 -- Calcular el número de semana manualmente como Power BI WEEKNUM(fecha,11)
#                 -- Sistema 11: Lunes es el primer día de la semana, primera semana contiene 1 de enero
#                 CASE 
#                     WHEN EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = {ANIO_ACTUAL} THEN
#                         FLOOR((DATE_DIFF(DATE(s.FechaIntegrada), DATE('{ANIO_ACTUAL}-01-01'), DAY) + 
#                                EXTRACT(DAYOFWEEK FROM DATE('{ANIO_ACTUAL}-01-01')) - 2) / 7) + 1
#                     WHEN EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = {ANIO_COMPARACION} THEN
#                         FLOOR((DATE_DIFF(DATE(s.FechaIntegrada), DATE('{ANIO_COMPARACION}-01-01'), DAY) + 
#                                EXTRACT(DAYOFWEEK FROM DATE('{ANIO_COMPARACION}-01-01')) - 2) / 7) + 1
#                     ELSE 1
#                 END AS semana_powerbi
#             FROM `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_VENTAS_ID}` s
#             JOIN `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_NEGOCIOS_ID}` n
#                 ON s.CodigoNegocio = n.CodigoNegocio
#             WHERE n.Descripcion = '{restaurante}'
#                 AND Estado = '0.0'
#                 AND (DATE(s.FechaIntegrada) >= '{ANIO_COMPARACION}-01-01' 
#                      AND DATE(s.FechaIntegrada) <= CURRENT_DATE())
#                 AND s.Monto IS NOT NULL
#                 AND CAST(s.Monto AS FLOAT64) > 0
#         )
#         SELECT 
#             anio,
#             semana_powerbi as semana,
#             COUNT(*) as num_transacciones,
#             SUM(monto) AS total_ventas,
#             MIN(fecha) as fecha_inicio_semana,
#             MAX(fecha) as fecha_fin_semana
#         FROM ventas_raw
#         WHERE semana_powerbi BETWEEN 1 AND 53
#         GROUP BY anio, semana_powerbi
#         HAVING SUM(monto) > 0
#         ORDER BY anio, semana_powerbi
#     '''
#     df = bigquery_client.query(query).to_dataframe()
#     return df


# def generar_reporte_comparativo_con_tabla(restaurante):
#     """Genera reporte comparativo dinámico usando las variables globales de años"""
    
#     # Obtener datos reales usando años dinámicos
#     df = obtener_ventas_semanales_powerbi_compatible(restaurante)
#     if df.empty:
#         print(f"No hay datos para {restaurante}")
#         return
    
#     df_actual = df[df['anio'] == ANIO_ACTUAL].copy()
#     if df_actual.empty:
#         print(f"No hay datos del {ANIO_ACTUAL}")
#         return
#       # *** USAR DATOS REALES DEL AÑO DE COMPARACIÓN ***
#     df_comparacion_real = obtener_datos_anio_comparacion_real(restaurante)
    
#     # Preparar datos para comparación - MOSTRAR TODAS LAS SEMANAS DESDE LA 1
#     semana_actual = datetime.now().isocalendar()[1]
    
#     print(f"Generando reporte comparativo para {restaurante}:")
#     print(f"- Datos {ANIO_ACTUAL} (reales): {len(df_actual)} semanas")
#     if not df_comparacion_real.empty:
#         print(f"- Datos {ANIO_COMPARACION} (reales): {len(df_comparacion_real)} semanas disponibles")
#         print(f"- Comparación justa: Ambos años hasta semana {semana_actual}")
    
#     # Preparar datos para comparación - MOSTRAR TODAS LAS SEMANAS DESDE LA 1
#     semanas_completas = list(range(1, semana_actual + 1))
    
#     # Series para ambos años
#     ventas_actual = pd.Series(index=semanas_completas, dtype=float).fillna(0)
#     ventas_comparacion = pd.Series(index=semanas_completas, dtype=float).fillna(0)
    
#     # Llenar con datos reales del año actual
#     for _, row in df_actual.iterrows():
#         if int(row['semana']) in ventas_actual.index:
#             ventas_actual[int(row['semana'])] = row['total_ventas']
    
#     # Llenar con datos REALES del año de comparación - SOLO HASTA LA SEMANA ACTUAL
#     for _, row in df_comparacion_real.iterrows():
#         semana_comp = int(row['semana'])
#         # Solo incluir semanas hasta la semana actual para comparación justa
#         if semana_comp in ventas_comparacion.index and semana_comp <= semana_actual:
#             ventas_comparacion[semana_comp] = row['total_ventas']
    
#     # Calcular acumulados
#     acum_actual = ventas_actual.cumsum()
#     acum_comparacion = ventas_comparacion.cumsum()
#       # Crear tabla de comparación con las columnas solicitadas
#     tabla_comparativa = pd.DataFrame({
#         'nro_semana': semanas_completas,
#         'ventas_actuales': ventas_actual.values,
#         'ventas_año_anterior': ventas_comparacion.values,
#         'diferencia': ventas_actual.values - ventas_comparacion.values,
#         'diferencia_porcentual': ((ventas_actual.values - ventas_comparacion.values) / ventas_comparacion.values * 100)
#     })
     
#     # Manejar divisiones por cero
#     tabla_comparativa['diferencia_porcentual'] = tabla_comparativa['diferencia_porcentual'].replace([float('inf'), -float('inf')], 0).fillna(0)
#       # Agregar acumulados
#     tabla_comparativa['acum_actuales'] = acum_actual.values
#     tabla_comparativa['acum_año_anterior'] = acum_comparacion.values
#     tabla_comparativa['dif_acum'] = acum_actual.values - acum_comparacion.values
#     tabla_comparativa['dif_acum_porcentual'] = ((acum_actual.values - acum_comparacion.values) / acum_comparacion.values * 100)
#     tabla_comparativa['dif_acum_porcentual'] = tabla_comparativa['dif_acum_porcentual'].replace([float('inf'), -float('inf')], 0).fillna(0)
    
#     # Crear figura con gráfico y tabla integrada - TAMAÑO AUMENTADO
#     fig = plt.figure(figsize=(24, 20))
    
#     # Layout: 50% gráfico, 50% tabla (aprovechar espacio)
#     gs = fig.add_gridspec(2, 1, height_ratios=[1, 1], hspace=0.15)
    
#     # === GRÁFICO PRINCIPAL ===
#     ax_main = fig.add_subplot(gs[0])
    
#     # Gráfico de barras comparativo
#     x = np.arange(len(semanas_completas))
#     width = 0.35
    
#     bars1 = ax_main.bar(x - width/2, tabla_comparativa['ventas_actuales'], width,
#                         label=f'Ventas {ANIO_ACTUAL} (Actuales)', color='#4CAF50', alpha=0.8)
#     bars2 = ax_main.bar(x + width/2, tabla_comparativa['ventas_año_anterior'], width,
#                         label=f'Ventas {ANIO_COMPARACION} (Referencia)', color='#FFC107', alpha=0.8)
    
#     # Eje secundario para acumulados
#     ax_secondary = ax_main.twinx()
#     ax_secondary.plot(x, tabla_comparativa['acum_actuales'], 
#                      color='#2E7D32', linewidth=3, marker='o', markersize=4,
#                      label=f'YTD {ANIO_ACTUAL} (acumulado)', linestyle='-')
#     ax_secondary.plot(x, tabla_comparativa['acum_año_anterior'],
#                      color='#F57C00', linewidth=3, marker='s', markersize=4,
#                      label=f'YTD {ANIO_COMPARACION} (acumulado)', linestyle='--')
    
#     # Configurar ejes
#     ax_main.set_xlabel('Semana del Año', fontsize=12)
#     ax_main.set_ylabel('Ventas Semanales (S/)', fontsize=12)
#     ax_secondary.set_ylabel('Ventas Acumuladas (S/)', fontsize=12)
#     ax_main.set_title(f'Evolución Anual de Ventas - Tendencia Comparativa {ANIO_COMPARACION} vs {ANIO_ACTUAL}', 
#                       fontsize=16, fontweight='bold', pad=20)
#       # Etiquetas del eje X - MOSTRAR TODAS LAS SEMANAS
#     ax_main.set_xticks(x)  # Mostrar todas las semanas
#     ax_main.set_xticklabels([f'S{i:02d}' for i in semanas_completas], rotation=45, fontsize=8)
    
#     # Leyenda combinada
#     lines1, labels1 = ax_main.get_legend_handles_labels()
#     lines2, labels2 = ax_secondary.get_legend_handles_labels()
#     ax_main.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=10)
    
#     ax_main.grid(True, alpha=0.3)
#       # === ESTADÍSTICAS RESUMIDAS ===
#     total_actual = tabla_comparativa['acum_actuales'].iloc[-1]
#     total_comparacion = tabla_comparativa['acum_año_anterior'].iloc[-1]
#     diferencia_total = total_actual - total_comparacion
#     porcentaje_total = (diferencia_total / total_comparacion * 100) if total_comparacion != 0 else 0
    
#     # Texto de estadísticas en el gráfico
#     stats_text = f"Total {ANIO_ACTUAL}: S/ {total_actual:,.0f} | Total {ANIO_COMPARACION}: S/ {total_comparacion:,.0f} | Diferencia: {porcentaje_total:+.1f}%"
#     ax_main.text(0.5, 0.95, stats_text, transform=ax_main.transAxes, 
#                 bbox=dict(boxstyle="round,pad=0.3", facecolor="lightblue", alpha=0.8),
#                 fontsize=12, fontweight='bold', ha='center')
#       # === TABLA DE DATOS ===
#     ax_table = fig.add_subplot(gs[1])
#     ax_table.axis('off')
#       # Preparar datos de la tabla - MOSTRAR TODAS LAS SEMANAS DESDE LA 1
#     tabla_display = tabla_comparativa.copy().round(0)  # TODAS las semanas, no solo las últimas
    
#     tabla_formateada = []
#     for _, row in tabla_display.iterrows():
#         tabla_formateada.append([
#             f"S{int(row['nro_semana']):02d}",
#             f"S/ {row['ventas_actuales']:,.0f}",
#             f"S/ {row['ventas_año_anterior']:,.0f}",
#             f"S/ {row['diferencia']:+,.0f}",
#             f"{row['diferencia_porcentual']:+.1f}%",
#             f"S/ {row['acum_actuales']:,.0f}",
#             f"S/ {row['acum_año_anterior']:,.0f}",
#             f"S/ {row['dif_acum']:+,.0f}",
#             f"{row['dif_acum_porcentual']:+.1f}%"
#         ])
    
#     # Crear tabla con las columnas solicitadas
#     headers = ['Nro\nSemana', 'Ventas\nActuales', 'Ventas Año\nAnterior', 'Diferencia', 'Diferencia\nPorcentual',
#                'Acum\nActuales', 'Acum Año\nAnterior', 'Dif\nAcumulada', 'Dif Acum\nPorcentual']
    
#     table = ax_table.table(cellText=tabla_formateada,
#                           colLabels=headers,
#                           cellLoc='center',
#                           loc='center',
#                           bbox=[0, 0, 1, 1])
    
#     # Formatear tabla
#     table.auto_set_font_size(False)
#     table.set_fontsize(9)
#     table.scale(1, 2)
    
#     # Colorear encabezados
#     for i in range(len(headers)):
#         table[(0, i)].set_facecolor('#1976D2')
#         table[(0, i)].set_text_props(weight='bold', color='white')
    
#     # Colorear filas y diferencias
#     for i in range(1, len(tabla_formateada) + 1):
#         for j in range(len(headers)):
#             # Filas alternadas
#             if i % 2 == 0:
#                 table[(i, j)].set_facecolor('#f8f9fa')
            
#             # Colorear diferencias
#             if j in [3, 4, 7, 8]:  # Columnas de diferencias
#                 try:
#                     valor_dif = tabla_display.iloc[i-1]['diferencia'] if j in [3, 4] else tabla_display.iloc[i-1]['dif_acum']
#                     if valor_dif > 0:
#                         table[(i, j)].set_facecolor('#e8f5e8')
#                         table[(i, j)].set_text_props(color='#2e7d32', weight='bold')
#                     elif valor_dif < 0:
#                         table[(i, j)].set_facecolor('#ffebee')
#                         table[(i, j)].set_text_props(color='#d32f2f', weight='bold')
#                 except:
#                     pass
#       # Título principal
#     fig.suptitle(f'Reporte Comparativo Completo - {restaurante}\n'
#                  f'Total {ANIO_ACTUAL}: S/ {total_actual:,.0f} | Total {ANIO_COMPARACION}: S/ {total_comparacion:,.0f} | '
#                  f'Diferencia: {porcentaje_total:+.1f}%', 
#                  fontsize=16, fontweight='bold', y=0.98)
    
#     # Guardar archivos
#     os.makedirs('reportes4', exist_ok=True)
#     plt.savefig(f'reportes4/reporte_comparativo_completo_{restaurante.replace(" ", "_").lower()}.png', 
#                 dpi=300, bbox_inches='tight')
#     plt.close()
      
#     # Guardar tabla completa en CSV
#     tabla_comparativa = []
#     # tabla_comparativa.to_csv(f'reportes3/tabla_comparativa_completa_{restaurante.replace(" ", "_").lower()}.csv', index=False)    
#     print(f'\n✅ Reporte comparativo completo generado para {restaurante}')
#     print(f'📊 Archivos creados:')
#     print(f'   - reporte_comparativo_completo_{restaurante.replace(" ", "_").lower()}.png')
#     print(f'   - tabla_comparativa_completa_{restaurante.replace(" ", "_").lower()}.csv')
#     print(f'\n📈 Resumen de resultados:')
#     print(f'   - Total ventas {ANIO_ACTUAL}: S/ {total_actual:,.0f}')
#     print(f'   - Total ventas {ANIO_COMPARACION}: S/ {total_comparacion:,.0f}')
#     print(f'   - Diferencia: S/ {diferencia_total:,.0f} ({porcentaje_total:+.1f}%)')
    
#     return tabla_comparativa


# def buscar_datos_reales_anio_comparacion_todos_restaurantes():
#     """Busca datos reales del año de comparación en todos los restaurantes para usar como referencia"""
#     query = f'''
#         WITH ventas_comparacion AS (
#             SELECT
#                 n.Descripcion AS nombre_restaurante,
#                 EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) AS anio,
#                 -- Calcular semana compatible con Power BI WEEKNUM(fecha,11)
#                 CASE 
#                     WHEN EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = {ANIO_COMPARACION} THEN
#                         FLOOR((DATE_DIFF(DATE(s.FechaIntegrada), DATE('{ANIO_COMPARACION}-01-01'), DAY) + 
#                                EXTRACT(DAYOFWEEK FROM DATE('{ANIO_COMPARACION}-01-01')) - 2) / 7) + 1
#                     ELSE 1
#                 END AS semana_powerbi,
#                 DATE(s.FechaIntegrada) AS fecha,
#                 CAST(s.Monto AS FLOAT64) AS monto
#             FROM `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_VENTAS_ID}` s
#             JOIN `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_NEGOCIOS_ID}` n
#                 ON s.CodigoNegocio = n.CodigoNegocio
#             WHERE Estado = '0.0'
#                 AND EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = {ANIO_COMPARACION}
#                 AND DATE(s.FechaIntegrada) >= '{ANIO_COMPARACION}-01-01'
#                 AND DATE(s.FechaIntegrada) <= '{ANIO_COMPARACION}-12-31'
#                 AND s.Monto IS NOT NULL
#                 AND CAST(s.Monto AS FLOAT64) > 0
#         )
#         SELECT 
#             nombre_restaurante,
#             semana_powerbi as semana,
#             COUNT(*) as num_transacciones,
#             SUM(monto) AS total_ventas,
#             MIN(fecha) as fecha_inicio_semana,
#             MAX(fecha) as fecha_fin_semana
#         FROM ventas_comparacion
#         WHERE semana_powerbi BETWEEN 1 AND 53
#         GROUP BY nombre_restaurante, semana_powerbi
#         HAVING SUM(monto) > 0
#         ORDER BY nombre_restaurante, semana_powerbi
#     '''
#     df = bigquery_client.query(query).to_dataframe()
#     return df

# def obtener_datos_anio_comparacion_real(restaurante_objetivo):
#     """Obtiene datos reales del año de comparación del restaurante objetivo o de restaurantes similares"""
    
#     # Primero intentar obtener datos del restaurante objetivo
#     query_objetivo = f'''
#         WITH ventas_comparacion AS (
#             SELECT
#                 n.Descripcion AS nombre_restaurante,
#                 EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) AS anio,
#                 CASE 
#                     WHEN EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = {ANIO_COMPARACION} THEN
#                         FLOOR((DATE_DIFF(DATE(s.FechaIntegrada), DATE('{ANIO_COMPARACION}-01-01'), DAY) + 
#                                EXTRACT(DAYOFWEEK FROM DATE('{ANIO_COMPARACION}-01-01')) - 2) / 7) + 1
#                     ELSE 1
#                 END AS semana_powerbi,
#                 CAST(s.Monto AS FLOAT64) AS monto
#             FROM `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_VENTAS_ID}` s
#             JOIN `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_NEGOCIOS_ID}` n
#                 ON s.CodigoNegocio = n.CodigoNegocio
#             WHERE n.Descripcion = '{restaurante_objetivo}'
#                 AND Estado = '0.0'
#                 AND EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = {ANIO_COMPARACION}
#                 AND s.Monto IS NOT NULL
#                 AND CAST(s.Monto AS FLOAT64) > 0
#         )
#         SELECT 
#             {ANIO_COMPARACION} as anio,
#             semana_powerbi as semana,
#             COUNT(*) as num_transacciones,
#             SUM(monto) AS total_ventas
#         FROM ventas_comparacion
#         WHERE semana_powerbi BETWEEN 1 AND 53
#         GROUP BY semana_powerbi
#         HAVING SUM(monto) > 0
#         ORDER BY semana_powerbi
#     '''
    
#     df_objetivo = bigquery_client.query(query_objetivo).to_dataframe()
    
#     if not df_objetivo.empty:
#         print(f"✅ Encontrados datos reales del {ANIO_COMPARACION} para {restaurante_objetivo}: {len(df_objetivo)} semanas")
#         return df_objetivo
    
#     # Si no hay datos del restaurante objetivo, buscar datos de otros restaurantes como referencia
#     print(f"⚠️ No se encontraron datos del {ANIO_COMPARACION} para {restaurante_objetivo}")
#     print(f"🔍 Buscando datos del {ANIO_COMPARACION} en otros restaurantes como referencia...")
    
#     df_todos = buscar_datos_reales_anio_comparacion_todos_restaurantes()
    
#     if not df_todos.empty:
#         # Usar el restaurante con más datos del año de comparación
#         restaurante_con_mas_datos = df_todos.groupby('nombre_restaurante')['total_ventas'].sum().idxmax()
#         df_referencia = df_todos[df_todos['nombre_restaurante'] == restaurante_con_mas_datos].copy()
#         df_referencia = df_referencia[['semana', 'total_ventas', 'num_transacciones']].copy()
#         df_referencia['anio'] = ANIO_COMPARACION
        
#         print(f"📊 Usando datos del {ANIO_COMPARACION} de '{restaurante_con_mas_datos}' como referencia: {len(df_referencia)} semanas")
#         return df_referencia
    
#     print(f"❌ No se encontraron datos del {ANIO_COMPARACION} en ningún restaurante")
#     return pd.DataFrame()

# # === CONFIGURACIÓN MENSUAL ===
# MES_ACTUAL = 6  # Junio
# ANIO_ACTUAL_MENSUAL = 2025
# ANIO_COMPARACION_MENSUAL = 2024
# NOMBRE_MES_ACTUAL = month_map[MES_ACTUAL]

# def configurar_analisis_mensual(anio_actual=None, mes_actual=None):
#     """
#     Configura el análisis mensual dinámicamente
#     Args:
#         anio_actual: Año actual para el análisis (por defecto 2025)
#         mes_actual: Mes actual para el análisis (1-12, por defecto 6=Junio)
    
#     Ejemplos:
#         configurar_analisis_mensual()  # Usar defaults: Junio 2025 vs Junio 2024
#         configurar_analisis_mensual(mes_actual=5)  # Analizar Mayo
#         configurar_analisis_mensual(anio_actual=2024, mes_actual=4)  # Abril 2024 vs 2023
#     """
#     global MES_ACTUAL, ANIO_ACTUAL_MENSUAL, ANIO_COMPARACION_MENSUAL, NOMBRE_MES_ACTUAL
    
#     if anio_actual:
#         ANIO_ACTUAL_MENSUAL = anio_actual
#         ANIO_COMPARACION_MENSUAL = anio_actual - 1
    
#     if mes_actual:
#         MES_ACTUAL = mes_actual
#         NOMBRE_MES_ACTUAL = month_map[mes_actual]
    
#     print(f"📅 Configuración mensual actualizada:")
#     print(f"   - Comparando: {NOMBRE_MES_ACTUAL} {ANIO_ACTUAL_MENSUAL} vs {NOMBRE_MES_ACTUAL} {ANIO_COMPARACION_MENSUAL}")


# def obtener_datos_mensuales(restaurante, anio, mes):
#     """Obtiene datos mensuales de ventas, transacciones y ticket promedio"""
#     query = f'''
#         SELECT
#             n.Descripcion AS restaurante,
#             EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) AS anio,
#             EXTRACT(MONTH FROM DATE(s.FechaIntegrada)) AS mes,
#             COUNT(*) AS total_transacciones,
#             SUM(CAST(s.Monto AS FLOAT64)) AS total_ventas,
#             AVG(CAST(s.Monto AS FLOAT64)) AS ticket_promedio,
#             -- Ventas por turno basadas en hora
#             SUM(CASE 
#                 WHEN EXTRACT(HOUR FROM DATETIME(s.FechaIntegrada)) BETWEEN 6 AND 11 
#                 THEN CAST(s.Monto AS FLOAT64) ELSE 0 END) AS ventas_manana,
#             SUM(CASE 
#                 WHEN EXTRACT(HOUR FROM DATETIME(s.FechaIntegrada)) BETWEEN 12 AND 17 
#                 THEN CAST(s.Monto AS FLOAT64) ELSE 0 END) AS ventas_tarde,
#             SUM(CASE 
#                 WHEN EXTRACT(HOUR FROM DATETIME(s.FechaIntegrada)) BETWEEN 18 AND 23 
#                 THEN CAST(s.Monto AS FLOAT64) ELSE 0 END) AS ventas_noche,
#             -- Transacciones por turno
#             COUNT(CASE 
#                 WHEN EXTRACT(HOUR FROM DATETIME(s.FechaIntegrada)) BETWEEN 6 AND 11 
#                 THEN 1 END) AS transacciones_manana,
#             COUNT(CASE 
#                 WHEN EXTRACT(HOUR FROM DATETIME(s.FechaIntegrada)) BETWEEN 12 AND 17 
#                 THEN 1 END) AS transacciones_tarde,
#             COUNT(CASE 
#                 WHEN EXTRACT(HOUR FROM DATETIME(s.FechaIntegrada)) BETWEEN 18 AND 23 
#                 THEN 1 END) AS transacciones_noche
#         FROM `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_VENTAS_ID}` s
#         JOIN `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_NEGOCIOS_ID}` n
#             ON s.CodigoNegocio = n.CodigoNegocio
#         WHERE n.Descripcion = '{restaurante}'
#             AND Estado = '0.0'
#             AND EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = {anio}
#             AND EXTRACT(MONTH FROM DATE(s.FechaIntegrada)) = {mes}
#             AND s.Monto IS NOT NULL
#             AND CAST(s.Monto AS FLOAT64) > 0
#         GROUP BY n.Descripcion, EXTRACT(YEAR FROM DATE(s.FechaIntegrada)), EXTRACT(MONTH FROM DATE(s.FechaIntegrada))
#     '''
    
#     df = bigquery_client.query(query).to_dataframe()
#     return df


# def extraer_metricas_mensuales(df_actual, df_comparacion):
#     """Extrae métricas de los dataframes y maneja valores NaN"""
#     def safe_value(df, column, default=0):
#         if df.empty or column not in df.columns:
#             return default
#         value = df[column].iloc[0] if len(df) > 0 else default
#         return value if pd.notna(value) and np.isfinite(value) else default
    
#     # Métricas año actual
#     ventas_actual = safe_value(df_actual, 'total_ventas')
#     transacciones_actual = safe_value(df_actual, 'total_transacciones')
#     ticket_actual = safe_value(df_actual, 'ticket_promedio')
    
#     # Métricas año anterior
#     ventas_anterior = safe_value(df_comparacion, 'total_ventas')
#     transacciones_anterior = safe_value(df_comparacion, 'total_transacciones')
#     ticket_anterior = safe_value(df_comparacion, 'ticket_promedio')
    
#     # Calcular variaciones
#     def calcular_variacion(actual, anterior):
#         if anterior == 0 or pd.isna(anterior) or not np.isfinite(anterior):
#             return 0
#         variacion = ((actual - anterior) / anterior) * 100
#         return variacion if pd.notna(variacion) and np.isfinite(variacion) else 0
    
#     var_ventas = calcular_variacion(ventas_actual, ventas_anterior)
#     var_transacciones = calcular_variacion(transacciones_actual, transacciones_anterior)
#     var_ticket = calcular_variacion(ticket_actual, ticket_anterior)
    
#     return {
#         'ventas_actual': ventas_actual,
#         'ventas_anterior': ventas_anterior,
#         'var_ventas': var_ventas,
#         'transacciones_actual': transacciones_actual,
#         'transacciones_anterior': transacciones_anterior,
#         'var_transacciones': var_transacciones,
#         'ticket_actual': ticket_actual,
#         'ticket_anterior': ticket_anterior,
#         'var_ticket': var_ticket
#     }


# def crear_cuadros_variaciones(metricas):
#     """Crea los cuadros de variaciones de ventas y ticket"""
    
#     # Cuadro de Variaciones de Ventas
#     cuadro_ventas = pd.DataFrame({
#         'Métrica': ['Ventas Totales', 'Transacciones', 'Ventas por Transacción'],
#         'Actual': [
#             f"S/ {metricas['ventas_actual']:,.0f}",
#             f"{metricas['transacciones_actual']:,.0f}",
#             f"S/ {metricas['ticket_actual']:,.2f}"
#         ],
#         'Año Anterior': [
#             f"S/ {metricas['ventas_anterior']:,.0f}",
#             f"{metricas['transacciones_anterior']:,.0f}",
#             f"S/ {metricas['ticket_anterior']:,.2f}"
#         ],
#         'Variación %': [
#             f"{metricas['var_ventas']:+.1f}%",
#             f"{metricas['var_transacciones']:+.1f}%",
#             f"{metricas['var_ticket']:+.1f}%"
#         ]
#     })
    
#     # Cuadro específico de Ticket Promedio
#     cuadro_ticket = pd.DataFrame({
#         'Período': [f'{NOMBRE_MES_ACTUAL} {ANIO_ACTUAL_MENSUAL}', f'{NOMBRE_MES_ACTUAL} {ANIO_COMPARACION_MENSUAL}'],
#         'Ticket Promedio': [
#             f"S/ {metricas['ticket_actual']:,.2f}",
#             f"S/ {metricas['ticket_anterior']:,.2f}"
#         ],
#         'Transacciones': [
#             f"{metricas['transacciones_actual']:,.0f}",
#             f"{metricas['transacciones_anterior']:,.0f}"
#         ],
#         'Ventas Totales': [
#             f"S/ {metricas['ventas_actual']:,.0f}",
#             f"S/ {metricas['ventas_anterior']:,.0f}"
#         ]
#     })
    
#     return cuadro_ventas, cuadro_ticket


# def crear_visualizacion_analisis_mensual(cuadro_ventas, cuadro_ticket, metricas, restaurante, df_actual, df_comparacion):
#     """Crea la visualización completa del análisis mensual"""
#     fig = plt.figure(figsize=(20, 16))
#     gs = fig.add_gridspec(3, 2, height_ratios=[1, 1, 1], hspace=0.3, wspace=0.3)
    
#     # === GRÁFICO COMPARATIVO DE BARRAS ===
#     ax1 = fig.add_subplot(gs[0, :])
    
#     categorias = ['Ventas Totales', 'Transacciones', 'Ticket Promedio']
#     valores_actual = [metricas['ventas_actual'], metricas['transacciones_actual'], metricas['ticket_actual']]
#     valores_anterior = [metricas['ventas_anterior'], metricas['transacciones_anterior'], metricas['ticket_anterior']]
#     variaciones = [metricas['var_ventas'], metricas['var_transacciones'], metricas['var_ticket']]
    
#     x = np.arange(len(categorias))
#     width = 0.35
    
#     bars1 = ax1.bar(x - width/2, valores_actual, width, label=f'{NOMBRE_MES_ACTUAL} {ANIO_ACTUAL_MENSUAL}', 
#                     color='#4CAF50', alpha=0.8)
#     bars2 = ax1.bar(x + width/2, valores_anterior, width, label=f'{NOMBRE_MES_ACTUAL} {ANIO_COMPARACION_MENSUAL}', 
#                     color='#FF9800', alpha=0.8)
    
#     # Agregar valores en las barras y variaciones
#     for i, (bar1, bar2, variacion) in enumerate(zip(bars1, bars2, variaciones)):
#         height1 = bar1.get_height()
#         height2 = bar2.get_height()
        
#         # Validar que los valores son finitos
#         height1 = height1 if pd.notna(height1) and np.isfinite(height1) else 0
#         height2 = height2 if pd.notna(height2) and np.isfinite(height2) else 0
#         variacion = variacion if pd.notna(variacion) and np.isfinite(variacion) else 0
        
#         ax1.text(bar1.get_x() + bar1.get_width()/2., height1,
#                 f'{height1:,.0f}', ha='center', va='bottom', fontweight='bold')
#         ax1.text(bar2.get_x() + bar2.get_width()/2., height2,
#                 f'{height2:,.0f}', ha='center', va='bottom', fontweight='bold')
        
#         # Mostrar variación porcentual
#         color_variacion = '#2E7D32' if variacion >= 0 else '#D32F2F'
#         max_height = max(height1, height2)
#         if max_height > 0:
#             ax1.text(i, max_height * 1.1, f'{variacion:+.1f}%',
#                     ha='center', va='bottom', fontweight='bold', color=color_variacion, fontsize=12)
    
#     ax1.set_xlabel('Métricas', fontsize=12)
#     ax1.set_ylabel('Valores', fontsize=12)
#     ax1.set_title(f'Comparación Mensual: {NOMBRE_MES_ACTUAL} {ANIO_ACTUAL_MENSUAL} vs {NOMBRE_MES_ACTUAL} {ANIO_COMPARACION_MENSUAL}', 
#                   fontsize=16, fontweight='bold')
#     ax1.set_xticks(x)
#     ax1.set_xticklabels(categorias)
#     ax1.legend()
#     ax1.grid(True, alpha=0.3)
    
#     # Definir variables comunes para los gráficos de turno
#     turnos = ['Mañana', 'Tarde', 'Noche']
#     colors = ['#FF9800', '#2196F3', '#9C27B0']
    
#     # === GRÁFICO DE VENTAS POR TURNO (AÑO ACTUAL) ===
#     if not df_actual.empty:
#         ax2 = fig.add_subplot(gs[1, 0])
        
#         ventas_turnos_actual = [
#             df_actual['ventas_manana'].iloc[0] if pd.notna(df_actual['ventas_manana'].iloc[0]) else 0,
#             df_actual['ventas_tarde'].iloc[0] if pd.notna(df_actual['ventas_tarde'].iloc[0]) else 0, 
#             df_actual['ventas_noche'].iloc[0] if pd.notna(df_actual['ventas_noche'].iloc[0]) else 0
#         ]
        
#         # Asegurar que los valores sean positivos y finitos
#         ventas_turnos_actual = [max(0, v) if pd.notna(v) and np.isfinite(v) else 0 for v in ventas_turnos_actual]
        
#         # Solo crear gráfico si hay ventas
#         if sum(ventas_turnos_actual) > 0:
#             wedges, texts, autotexts = ax2.pie(ventas_turnos_actual, labels=turnos, autopct='%1.1f%%',
#                                               colors=colors, startangle=90)
            
#             ax2.set_title(f'Distribución por Turno\n{NOMBRE_MES_ACTUAL} {ANIO_ACTUAL_MENSUAL}', 
#                           fontsize=14, fontweight='bold')
#         else:
#             ax2.text(0.5, 0.5, 'Sin datos\ndisponibles', transform=ax2.transAxes,
#                     ha='center', va='center', fontsize=14, fontweight='bold')
#             ax2.set_title(f'Distribución por Turno\n{NOMBRE_MES_ACTUAL} {ANIO_ACTUAL_MENSUAL}', 
#                           fontsize=14, fontweight='bold')
#     else:
#         ax2 = fig.add_subplot(gs[1, 0])
#         ax2.text(0.5, 0.5, 'Sin datos\ndisponibles', transform=ax2.transAxes,
#                 ha='center', va='center', fontsize=14, fontweight='bold')
#         ax2.set_title(f'Distribución por Turno\n{NOMBRE_MES_ACTUAL} {ANIO_ACTUAL_MENSUAL}', 
#                       fontsize=14, fontweight='bold')
    
#     # === GRÁFICO DE VENTAS POR TURNO (AÑO ANTERIOR) ===
#     if not df_comparacion.empty and len(df_comparacion) > 0:
#         ax3 = fig.add_subplot(gs[1, 1])
        
#         ventas_turnos_anterior = [
#             df_comparacion['ventas_manana'].iloc[0] if pd.notna(df_comparacion['ventas_manana'].iloc[0]) else 0,
#             df_comparacion['ventas_tarde'].iloc[0] if pd.notna(df_comparacion['ventas_tarde'].iloc[0]) else 0, 
#             df_comparacion['ventas_noche'].iloc[0] if pd.notna(df_comparacion['ventas_noche'].iloc[0]) else 0
#         ]
        
#         # Asegurar que los valores sean positivos y finitos
#         ventas_turnos_anterior = [max(0, v) if pd.notna(v) and np.isfinite(v) else 0 for v in ventas_turnos_anterior]
        
#         # Solo crear gráfico si hay ventas
#         if sum(ventas_turnos_anterior) > 0:
#             wedges, texts, autotexts = ax3.pie(ventas_turnos_anterior, labels=turnos, autopct='%1.1f%%',
#                                               colors=colors, startangle=90)
            
#             ax3.set_title(f'Distribución por Turno\n{NOMBRE_MES_ACTUAL} {ANIO_COMPARACION_MENSUAL}', 
#                           fontsize=14, fontweight='bold')
#         else:
#             ax3.text(0.5, 0.5, 'Sin datos\ndisponibles', transform=ax3.transAxes,
#                     ha='center', va='center', fontsize=14, fontweight='bold')
#             ax3.set_title(f'Distribución por Turno\n{NOMBRE_MES_ACTUAL} {ANIO_COMPARACION_MENSUAL}', 
#                           fontsize=14, fontweight='bold')
#     else:
#         ax3 = fig.add_subplot(gs[1, 1])
#         ax3.text(0.5, 0.5, 'Sin datos\ndisponibles', transform=ax3.transAxes,
#                 ha='center', va='center', fontsize=14, fontweight='bold')
#         ax3.set_title(f'Distribución por Turno\n{NOMBRE_MES_ACTUAL} {ANIO_COMPARACION_MENSUAL}', 
#                       fontsize=14, fontweight='bold')
    
#     # === TABLA DE VARIACIONES (CUADRO PRINCIPAL) ===
#     ax4 = fig.add_subplot(gs[2, 0])
#     ax4.axis('off')
    
#     # Preparar datos para la tabla
#     table_data = []
#     for _, row in cuadro_ventas.iterrows():
#         table_data.append([row['Métrica'], row['Actual'], row['Año Anterior'], row['Variación %']])
    
#     table1 = ax4.table(cellText=table_data,
#                       colLabels=['Métrica', 'Actual', 'Año Anterior', 'Variación %'],
#                       cellLoc='center',
#                       loc='center',
#                       bbox=[0, 0, 1, 1])
    
#     table1.auto_set_font_size(False)
#     table1.set_fontsize(10)
#     table1.scale(1, 2)
    
#     # Colorear encabezados
#     for i in range(4):
#         table1[(0, i)].set_facecolor('#1976D2')
#         table1[(0, i)].set_text_props(weight='bold', color='white')
    
#     # Colorear celdas de variación según valor
#     for i in range(1, len(table_data) + 1):
#         variacion_text = table_data[i-1][3]  # Columna de variación
#         if '+' in variacion_text:
#             table1[(i, 3)].set_facecolor('#E8F5E8')  # Verde claro para positivo
#         elif '-' in variacion_text:
#             table1[(i, 3)].set_facecolor('#FFEBEE')  # Rojo claro para negativo
    
#     ax4.set_title('Cuadro de Variaciones de Ventas', fontsize=14, fontweight='bold', pad=20)
    
#     # === TABLA DE TICKET PROMEDIO ===
#     ax5 = fig.add_subplot(gs[2, 1])
#     ax5.axis('off')
    
#     # Preparar datos para la tabla de ticket
#     table_data_ticket = []
#     for _, row in cuadro_ticket.iterrows():
#         table_data_ticket.append([row['Período'], row['Ticket Promedio'], 
#                                  row['Transacciones'], row['Ventas Totales']])
    
#     table2 = ax5.table(cellText=table_data_ticket,
#                       colLabels=['Período', 'Ticket Promedio', 'Transacciones', 'Ventas Totales'],
#                       cellLoc='center',
#                       loc='center',
#                       bbox=[0, 0, 1, 1])
    
#     table2.auto_set_font_size(False)
#     table2.set_fontsize(10)
#     table2.scale(1, 2)
    
#     # Colorear encabezados
#     for i in range(4):
#         table2[(0, i)].set_facecolor('#1976D2')
#         table2[(0, i)].set_text_props(weight='bold', color='white')
    
#     # ax5.set_title('Análisis de Ticket Promedio', fontsize=14, fontweight='bold', pad=20)
    
#     # # Guardar el gráfico
#     # plt.suptitle(f'Análisis Mensual - {restaurante} - {NOMBRE_MES_ACTUAL} {ANIO_ACTUAL_MENSUAL} vs {ANIO_COMPARACION_MENSUAL}', 
#     #              fontsize=18, fontweight='bold', y=0.98)
    
#     filename = f'reportes4/analisis_mensual_{restaurante.lower().replace(" ", "_")}_{NOMBRE_MES_ACTUAL.lower()}_{ANIO_ACTUAL_MENSUAL}_vs_{ANIO_COMPARACION_MENSUAL}.png'
#     os.makedirs('reportes4', exist_ok=True)
#     plt.savefig(filename, dpi=300, bbox_inches='tight')
#     # plt.show()
    
#     print(f"\n✅ Gráfico guardado como: {filename}")
#     return cuadro_ventas, cuadro_ticket


# def obtener_datos_anuales_por_mes(restaurante, anio):
#     """Obtiene datos mensuales de todo el año para un restaurante específico"""
#     query = f'''
#         SELECT
#             n.Descripcion AS restaurante,
#             EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) AS anio,
#             EXTRACT(MONTH FROM DATE(s.FechaIntegrada)) AS mes,
#             COUNT(*) AS total_transacciones,
#             SUM(CAST(s.Monto AS FLOAT64)) AS total_ventas,
#             -- Ticket promedio = total_ventas / total_transacciones
#             CASE 
#                 WHEN COUNT(*) > 0 
#                 THEN SUM(CAST(s.Monto AS FLOAT64)) / COUNT(*)
#                 ELSE 0 
#             END AS ticket_promedio,
#             -- También calcular con cantidad si está disponible
#             SUM(CASE WHEN s.Cantidad IS NOT NULL AND CAST(s.Cantidad AS FLOAT64) > 0 
#                 THEN CAST(s.Cantidad AS FLOAT64) ELSE 0 END) AS total_cantidad
#         FROM `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_VENTAS_ID}` s
#         JOIN `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_NEGOCIOS_ID}` n
#             ON s.CodigoNegocio = n.CodigoNegocio
#         WHERE n.Descripcion = '{restaurante}'
#             AND Estado = '0.0'
#             AND EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = {anio}
#             AND s.Monto IS NOT NULL
#             AND CAST(s.Monto AS FLOAT64) > 0
#         GROUP BY n.Descripcion, EXTRACT(YEAR FROM DATE(s.FechaIntegrada)), EXTRACT(MONTH FROM DATE(s.FechaIntegrada))        ORDER BY mes
#     '''
    
#     df = bigquery_client.query(query).to_dataframe()
    
#     # Mostrar resumen simple de datos obtenidos
#     if not df.empty:
#         print(f"📊 Datos obtenidos para {restaurante} - {anio}: {len(df)} meses con datos")
#     else:
#         print(f"⚠️ No se encontraron datos para {restaurante} en {anio}")
    
#     return df


# def crear_tablas_comparativas_anuales(df_actual, df_anterior):
#     """Crea tablas comparativas de ventas y ticket promedio por mes"""
    
#     # Solo mostrar hasta el mes actual del año actual
#     mes_limite = MES_ACTUAL  # Usar la variable global del mes actual
#     meses_completos = list(range(1, mes_limite + 1))
#     nombres_meses = [month_map[mes] for mes in meses_completos]
    
#     # Preparar datos de ventas
#     ventas_actuales = []
#     ventas_anteriores = []
#     tickets_actuales = []
#     tickets_anteriores = []
    
#     for mes in meses_completos:
#         # Ventas actuales
#         venta_actual = 0
#         ticket_actual = 0
#         if not df_actual.empty:
#             mes_data = df_actual[df_actual['mes'] == mes]
#             if not mes_data.empty:
#                 venta_actual = mes_data['total_ventas'].iloc[0]
#                 ticket_actual = mes_data['ticket_promedio'].iloc[0]
        
#         # Ventas anteriores
#         venta_anterior = 0
#         ticket_anterior = 0
#         if not df_anterior.empty:
#             mes_data = df_anterior[df_anterior['mes'] == mes]
#             if not mes_data.empty:
#                 venta_anterior = mes_data['total_ventas'].iloc[0]
#                 ticket_anterior = mes_data['ticket_promedio'].iloc[0]
        
#         ventas_actuales.append(venta_actual)
#         ventas_anteriores.append(venta_anterior)
#         tickets_actuales.append(ticket_actual)
#         tickets_anteriores.append(ticket_anterior)
    
#     # Calcular diferencias y porcentajes
#     def calcular_variaciones(actuales, anteriores):
#         diferencias = []
#         diferencias_pct = []
        
#         for actual, anterior in zip(actuales, anteriores):
#             diferencia = actual - anterior
#             diferencias.append(diferencia)
            
#             if anterior != 0 and not pd.isna(anterior):
#                 pct = (diferencia / anterior) * 100
#             else:
#                 pct = 0
#             diferencias_pct.append(pct)
        
#         return diferencias, diferencias_pct
    
#     # Tabla de ventas
#     dif_ventas, dif_ventas_pct = calcular_variaciones(ventas_actuales, ventas_anteriores)
    
#     tabla_ventas = pd.DataFrame({
#         'Mes': nombres_meses,
#         'Actual': [f"S/ {v:,.0f}" for v in ventas_actuales],
#         'Año Anterior': [f"S/ {v:,.0f}" for v in ventas_anteriores],
#         'Diferencia': [f"S/ {d:+,.0f}" for d in dif_ventas],
#         'Diferencia %': [f"{p:+.1f}%" for p in dif_ventas_pct],
#         'Actual_Num': ventas_actuales,  # Para gráficos
#         'Anterior_Num': ventas_anteriores
#     })
    
#     # Tabla de tickets
#     dif_tickets, dif_tickets_pct = calcular_variaciones(tickets_actuales, tickets_anteriores)
    
#     tabla_tickets = pd.DataFrame({
#         'Mes': nombres_meses,
#         'Actual': [f"S/ {t:.2f}" for t in tickets_actuales],
#         'Año Anterior': [f"S/ {t:.2f}" for t in tickets_anteriores],
#         'Diferencia': [f"S/ {d:+.2f}" for d in dif_tickets],
#         'Diferencia %': [f"{p:+.1f}%" for p in dif_tickets_pct],
#         'Actual_Num': tickets_actuales,  # Para gráficos
#         'Anterior_Num': tickets_anteriores
#     })
    
#     return tabla_ventas, tabla_tickets


# def crear_graficos_comparativos_anuales(tabla_ventas, tabla_tickets, restaurante):
#     import matplotlib.pyplot as plt
#     import os
    
#     # Solo mostrar hasta el mes actual
#     mes_limite = MES_ACTUAL
#     meses_num = list(range(1, mes_limite + 1))
#     nombres_meses_cortos = [m[:3] for m in tabla_ventas['Mes'][:mes_limite]]
    
#     fig = plt.figure(figsize=(20, 16))
#     gs = fig.add_gridspec(4, 1, height_ratios=[1, 1, 0.6, 0.6], hspace=0.4)

#     # Gráfico de ventas - solo hasta mes actual
#     ax1 = fig.add_subplot(gs[0])
#     ventas_actual_limitado = tabla_ventas['Actual_Num'][:mes_limite]
#     ventas_anterior_limitado = tabla_ventas['Anterior_Num'][:mes_limite]
    
#     ax1.plot(meses_num, ventas_actual_limitado, marker='o', linewidth=3, markersize=8,
#              label=f'Ventas {ANIO_ACTUAL_MENSUAL}', color='#4CAF50', markerfacecolor='white', markeredgewidth=2)
#     ax1.plot(meses_num, ventas_anterior_limitado, marker='s', linewidth=3, markersize=8,
#              label=f'Ventas {ANIO_COMPARACION_MENSUAL}', color='#FF9800', markerfacecolor='white', markeredgewidth=2)
    
#     ax1.set_title(f'Evolución de Ventas Mensuales - {restaurante}\n(Enero - {month_map[mes_limite]} {ANIO_ACTUAL_MENSUAL} vs {ANIO_COMPARACION_MENSUAL})', 
#                   fontsize=16, fontweight='bold')
#     ax1.set_xticks(meses_num)
#     ax1.set_xticklabels(nombres_meses_cortos)
#     ax1.set_ylabel('Ventas (S/)', fontsize=12)
#     ax1.legend(fontsize=12)
#     ax1.grid(True, alpha=0.3)
    
#     # Formatear eje Y con separadores de miles
#     ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'S/ {x:,.0f}'))
    
#     # Agregar valores en los puntos para mejor visualización
#     for i, (mes, valor) in enumerate(zip(meses_num, ventas_actual_limitado)):
#         if valor > 0:
#             ax1.annotate(f'S/ {valor:,.0f}', 
#                         (mes, valor), 
#                         textcoords="offset points", 
#                         xytext=(0,10), 
#                         ha='center', fontsize=9, fontweight='bold',
#                         color='#2E7D32')

#     # Gráfico de ticket promedio - solo hasta mes actual
#     ax2 = fig.add_subplot(gs[1])
#     tickets_actual_limitado = tabla_tickets['Actual_Num'][:mes_limite]
#     tickets_anterior_limitado = tabla_tickets['Anterior_Num'][:mes_limite]
    
#     ax2.plot(meses_num, tickets_actual_limitado, marker='o', linewidth=3, markersize=8,
#              label=f'Ticket Promedio {ANIO_ACTUAL_MENSUAL}', color='#2196F3', markerfacecolor='white', markeredgewidth=2)
#     ax2.plot(meses_num, tickets_anterior_limitado, marker='s', linewidth=3, markersize=8,
#              label=f'Ticket Promedio {ANIO_COMPARACION_MENSUAL}', color='#9C27B0', markerfacecolor='white', markeredgewidth=2)
    
#     ax2.set_title(f'Evolución del Ticket Promedio - {restaurante}\n(Enero - {month_map[mes_limite]} {ANIO_ACTUAL_MENSUAL} vs {ANIO_COMPARACION_MENSUAL})', 
#                   fontsize=16, fontweight='bold')
#     ax2.set_xticks(meses_num)
#     ax2.set_xticklabels(nombres_meses_cortos)
#     ax2.set_ylabel('Ticket Promedio (S/)', fontsize=12)
#     ax2.legend(fontsize=12)
#     ax2.grid(True, alpha=0.3)
    
#     # Formatear eje Y
#     ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'S/ {x:.2f}'))
    
#     # Agregar valores en los puntos
#     for i, (mes, valor) in enumerate(zip(meses_num, tickets_actual_limitado)):
#         if valor > 0:
#             ax2.annotate(f'S/ {valor:.2f}', 
#                         (mes, valor), 
#                         textcoords="offset points", 
#                         xytext=(0,10), 
#                         ha='center', fontsize=9, fontweight='bold',
#                         color='#1565C0')

#     # Tabla de ventas mejorada - solo hasta mes actual
#     ax3 = fig.add_subplot(gs[2])
#     ax3.axis('off')
#     tabla_ventas_limitada = tabla_ventas[:mes_limite]
#     tabla_ventas_display = tabla_ventas_limitada[['Mes', 'Actual', 'Año Anterior', 'Diferencia', 'Diferencia %']]
    
#     table1 = ax3.table(cellText=tabla_ventas_display.values,
#                       colLabels=tabla_ventas_display.columns,
#                       cellLoc='center', loc='center', bbox=[0, 0, 1, 1])
#     table1.auto_set_font_size(False)
#     table1.set_fontsize(10)
#     table1.scale(1, 1.5)
    
#     # Colorear encabezados
#     for i in range(len(tabla_ventas_display.columns)):
#         table1[(0, i)].set_facecolor('#1976D2')
#         table1[(0, i)].set_text_props(weight='bold', color='white')
    
#     # Colorear celdas según diferencias
#     for i in range(1, len(tabla_ventas_limitada) + 1):
#         for j in range(len(tabla_ventas_display.columns)):
#             if i % 2 == 0:
#                 table1[(i, j)].set_facecolor('#f8f9fa')
            
#             # Colorear diferencias porcentuales
#             if j == 4:  # Columna de Diferencia %
#                 diff_text = tabla_ventas_display.iloc[i-1]['Diferencia %']
#                 if '+' in str(diff_text):
#                     table1[(i, j)].set_facecolor('#e8f5e8')
#                     table1[(i, j)].set_text_props(color='#2e7d32', weight='bold')
#                 elif '-' in str(diff_text):
#                     table1[(i, j)].set_facecolor('#ffebee')
#                     table1[(i, j)].set_text_props(color='#d32f2f', weight='bold')
    
#     ax3.set_title('Tabla de Ventas Mensuales (Comparativo)', fontsize=14, fontweight='bold', pad=15)

#     # Tabla de ticket promedio mejorada - solo hasta mes actual
#     ax4 = fig.add_subplot(gs[3])
#     ax4.axis('off')
#     tabla_tickets_limitada = tabla_tickets[:mes_limite]
#     tabla_tickets_display = tabla_tickets_limitada[['Mes', 'Actual', 'Año Anterior', 'Diferencia', 'Diferencia %']]
    
#     table2 = ax4.table(cellText=tabla_tickets_display.values,
#                       colLabels=tabla_tickets_display.columns,
#                       cellLoc='center', loc='center', bbox=[0, 0, 1, 1])
#     table2.auto_set_font_size(False)
#     table2.set_fontsize(10)
#     table2.scale(1, 1.5)
    
#     # Colorear encabezados
#     for i in range(len(tabla_tickets_display.columns)):
#         table2[(0, i)].set_facecolor('#1976D2')
#         table2[(0, i)].set_text_props(weight='bold', color='white')
    
#     ax4.set_title('Análisis de Ticket Promedio Mensual (Comparativo)', fontsize=14, fontweight='bold', pad=15)

#     plt.suptitle(f'Análisis Anual Comparativo - {restaurante}\n(Período: Enero - {month_map[mes_limite]} | {ANIO_ACTUAL_MENSUAL} vs {ANIO_COMPARACION_MENSUAL})', 
#                  fontsize=18, fontweight='bold', y=0.98)
    
#     filename = f'reportes4/analisis_anual_comparativo_{restaurante.lower().replace(" ", "_")}_{ANIO_ACTUAL_MENSUAL}_vs_{ANIO_COMPARACION_MENSUAL}.png'
#     os.makedirs('reportes4', exist_ok=True)
#     plt.savefig(filename, dpi=300, bbox_inches='tight')
#     plt.close(fig)
#     print(f"\n✅ Gráfico anual guardado como: {filename}")
    
#     # Mostrar resumen de datos verificados
#     print(f"\n📊 VERIFICACIÓN DE DATOS:")
#     print(f"   - Período analizado: Enero - {month_map[mes_limite]} ({mes_limite} meses)")
#     print(f"   - Total ventas {ANIO_ACTUAL_MENSUAL}: S/ {sum(tabla_ventas['Actual_Num'][:mes_limite]):,.0f}")
#     print(f"   - Total ventas {ANIO_COMPARACION_MENSUAL}: S/ {sum(tabla_ventas['Anterior_Num'][:mes_limite]):,.0f}")
#     diferencia_total = sum(tabla_ventas['Actual_Num'][:mes_limite]) - sum(tabla_ventas['Anterior_Num'][:mes_limite])
#     print(f"   - Diferencia total: S/ {diferencia_total:+,.0f}")
    
#     return


# def diagnosticar_datos_faltantes(restaurante, anio):
#     """Función de diagnóstico para verificar qué datos existen en la base de datos"""
#     print(f"\n🔍 DIAGNÓSTICO DE DATOS PARA {restaurante} - {anio}")
#     print("=" * 60)
    
#     # Consulta básica sin filtros estrictos
#     query_diagnostico = f'''
#         SELECT
#             EXTRACT(MONTH FROM DATE(s.FechaIntegrada)) AS mes,
#             COUNT(*) as total_registros,
#             COUNT(CASE WHEN s.Monto IS NOT NULL AND CAST(s.Monto AS FLOAT64) > 0 THEN 1 END) as registros_con_monto,
#             COUNT(CASE WHEN s.Cantidad IS NOT NULL AND CAST(s.Cantidad AS FLOAT64) > 0 THEN 1 END) as registros_con_cantidad,
#             SUM(CASE WHEN s.Monto IS NOT NULL THEN CAST(s.Monto AS FLOAT64) ELSE 0 END) as suma_montos,
#             MIN(DATE(s.FechaIntegrada)) as fecha_minima,
#             MAX(DATE(s.FechaIntegrada)) as fecha_maxima
#         FROM `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_VENTAS_ID}` s
#         JOIN `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_NEGOCIOS_ID}` n
#             ON s.CodigoNegocio = n.CodigoNegocio
#         WHERE n.Descripcion = '{restaurante}'
#             AND EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = {anio}
#             AND EXTRACT(MONTH FROM DATE(s.FechaIntegrada)) IN (1, 2, 3, 4, 5, 6)
#         GROUP BY EXTRACT(MONTH FROM DATE(s.FechaIntegrada))
#         ORDER BY mes
#     '''
    
#     df_diagnostico = bigquery_client.query(query_diagnostico).to_dataframe()
    
#     if not df_diagnostico.empty:
#         for _, row in df_diagnostico.iterrows():
#             mes_nombre = month_map[int(row['mes'])]
#             print(f"{mes_nombre:>10}: {row['total_registros']:>6} registros | "
#                   f"Con monto: {row['registros_con_monto']:>6} | "
#                   f"Con cantidad: {row['registros_con_cantidad']:>6} | "
#                   f"Suma: S/ {row['suma_montos']:>10,.0f}")
#             print(f"{'':>10}  Fechas: {row['fecha_minima']} a {row['fecha_maxima']}")
#     else:            print(f"❌ No se encontraron registros para {restaurante} en {anio}")
    
#     print("=" * 60)
#     return df_diagnostico


# # === CONFIGURACIÓN MENSUAL ===
# MES_ACTUAL = 6  # Junio
# ANIO_ACTUAL_MENSUAL = 2025
# ANIO_COMPARACION_MENSUAL = 2024
# NOMBRE_MES_ACTUAL = month_map[MES_ACTUAL]

# def configurar_analisis_mensual(anio_actual=None, mes_actual=None):
#     """
#     Configura el análisis mensual dinámicamente
#     Args:
#         anio_actual: Año actual para el análisis (por defecto 2025)
#         mes_actual: Mes actual para el análisis (1-12, por defecto 6=Junio)
    
#     Ejemplos:
#         configurar_analisis_mensual()  # Usar defaults: Junio 2025 vs Junio 2024
#         configurar_analisis_mensual(mes_actual=5)  # Analizar Mayo
#         configurar_analisis_mensual(anio_actual=2024, mes_actual=4)  # Abril 2024 vs 2023
#     """
#     global MES_ACTUAL, ANIO_ACTUAL_MENSUAL, ANIO_COMPARACION_MENSUAL, NOMBRE_MES_ACTUAL
    
#     if anio_actual:
#         ANIO_ACTUAL_MENSUAL = anio_actual
#         ANIO_COMPARACION_MENSUAL = anio_actual - 1
    
#     if mes_actual:
#         MES_ACTUAL = mes_actual
#         NOMBRE_MES_ACTUAL = month_map[mes_actual]
    
#     print(f"� Configuración mensual actualizada:")
#     print(f"   - Comparando: {NOMBRE_MES_ACTUAL} {ANIO_ACTUAL_MENSUAL} vs {NOMBRE_MES_ACTUAL} {ANIO_COMPARACION_MENSUAL}")


# if __name__ == "__main__":
#     print("=== GENERADOR DE ANÁLISIS ANUAL COMPARATIVO ===")
#     print(f"Restaurante objetivo: {RESTAURANTE_OBJETIVO}")
#     print(f"Análisis: {ANIO_ACTUAL_MENSUAL} vs {ANIO_COMPARACION_MENSUAL}")
    
#     print("\n🔧 Configuración dinámica disponible:")
#     print("   - configurar_analisis_mensual()  # Usar valores por defecto")
#     print("   - configurar_analisis_mensual(anio_actual=2024)  # Analizar 2024 vs 2023")
    
#     print("\n" + "="*60)
#     print("EJECUTANDO ANÁLISIS ANUAL COMPARATIVO:")
#     print("="*60)
    
#     # Análisis anual comparativo (gráficos de líneas y tablas)
#     print("\n📊 ANÁLISIS ANUAL COMPARATIVO")
#     print("-" * 40)
#     generar_analisis_anual_comparativo(RESTAURANTE_OBJETIVO)
    
#     print("\n" + "="*60)
#     print("✅ ANÁLISIS FINALIZADO")
#     print("📁 Revisa la carpeta 'reportes4' para ver los gráficos de líneas y tablas generados")
#     print("="*60)