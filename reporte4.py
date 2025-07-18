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
OUTPUT_FOLDER = 'reportes4'
# N8N PATH SAVE REPORTS
# OUTPUT_FOLDER = '/data/scripts/reportes4'

TABLE_VENTAS_ID = 'sales_df'
TABLE_CATEGORIAS_ID = 'categorias'
TABLE_METAS_ID = 'MontosMeta'
TABLE_NEGOCIOS_ID = 'Negocios'

# BigQuery client
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
bigquery_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

# === CONFIGURACIÓN MENSUAL ===
# MES_ACTUAL = datetime.today().month
MES_ACTUAL =  datetime.today().month if datetime.today().day > 28 else (datetime.today().month - 1 if datetime.today().month > 1 else 12) 
ANIO_ACTUAL_MENSUAL = datetime.today().year
ANIO_COMPARACION_MENSUAL = ANIO_ACTUAL_MENSUAL - 1

# RESTAURANTE_OBJETIVO = 'Patio Cavenecia'
NOMBRE_MES_ACTUAL = month_map[MES_ACTUAL]

def configurar_analisis_mensual(anio_actual=None, mes_actual=None):
    """
    Configura el análisis mensual dinámicamente
    """
    global MES_ACTUAL, ANIO_ACTUAL_MENSUAL, ANIO_COMPARACION_MENSUAL, NOMBRE_MES_ACTUAL
    
    if anio_actual:
        ANIO_ACTUAL_MENSUAL = anio_actual
        ANIO_COMPARACION_MENSUAL = anio_actual - 1
    
    if mes_actual:
        MES_ACTUAL = mes_actual
        NOMBRE_MES_ACTUAL = month_map[mes_actual]
    
    print(f"Configuración mensual actualizada:")
    print(f"   - Comparando: {NOMBRE_MES_ACTUAL} {ANIO_ACTUAL_MENSUAL} vs {NOMBRE_MES_ACTUAL} {ANIO_COMPARACION_MENSUAL}")


def obtener_datos_anuales_por_mes(restaurante, anio):
    """Obtiene datos mensuales de todo el año para un restaurante específico"""
    
    # DIAGNÓSTICO: Verificar qué restaurantes existen en la base de datos
    if anio == ANIO_COMPARACION_MENSUAL:  # Solo para el año anterior
        print(f"\n🔍 DIAGNÓSTICO: Verificando restaurantes disponibles en {anio}...")
        query_restaurantes = f'''
            SELECT DISTINCT n.Descripcion as restaurante, COUNT(*) as registros
            FROM `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_VENTAS_ID}` s
            JOIN `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_NEGOCIOS_ID}` n
                ON s.CodigoNegocio = n.CodigoNegocio
            WHERE EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = {anio}
                AND Estado = '0.0'
            GROUP BY n.Descripcion
            ORDER BY registros DESC
            LIMIT 10
        '''
        df_restaurantes = bigquery_client.query(query_restaurantes).to_dataframe()
        if not df_restaurantes.empty:
            print("Restaurantes disponibles en la base de datos:")
            for _, row in df_restaurantes.iterrows():
                marca = "✓" if restaurante.lower() in row['restaurante'].lower() else " "
                print(f"  {marca} {row['restaurante']} ({row['registros']} registros)")
        
        # Verificar coincidencias parciales
        query_coincidencias = f'''
            SELECT DISTINCT n.Descripcion as restaurante, COUNT(*) as registros
            FROM `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_VENTAS_ID}` s
            JOIN `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_NEGOCIOS_ID}` n
                ON s.CodigoNegocio = n.CodigoNegocio
            WHERE EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = {anio}
                AND Estado = '0.0'
                AND LOWER(n.Descripcion) LIKE '%{restaurante.lower()}%'
            GROUP BY n.Descripcion
        '''
        df_coincidencias = bigquery_client.query(query_coincidencias).to_dataframe()
        if not df_coincidencias.empty:
            print(f"\nCoincidencias encontradas para '{restaurante}':")
            for _, row in df_coincidencias.iterrows():
                print(f"  ✓ {row['restaurante']} ({row['registros']} registros)")
      # CONSULTA PRINCIPAL - Corregida para usar Cantidad en el ticket promedio
    query = f'''
        SELECT
            n.Descripcion AS restaurante,
            EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) AS anio,
            EXTRACT(MONTH FROM DATE(s.FechaIntegrada)) AS mes,
            COUNT(*) AS total_registros,
            SUM(CAST(s.Monto AS FLOAT64)) AS total_ventas,
            SUM(CASE WHEN s.Cantidad IS NOT NULL AND CAST(s.Cantidad AS FLOAT64) > 0 
                THEN CAST(s.Cantidad AS FLOAT64) ELSE 1 END) AS total_transacciones,
            -- Ticket promedio CORREGIDO = total_ventas / total_transacciones (usando Cantidad)
            CASE 
                WHEN SUM(CASE WHEN s.Cantidad IS NOT NULL AND CAST(s.Cantidad AS FLOAT64) > 0 
                         THEN CAST(s.Cantidad AS FLOAT64) ELSE 1 END) > 0 
                THEN SUM(CAST(s.Monto AS FLOAT64)) / SUM(CASE WHEN s.Cantidad IS NOT NULL AND CAST(s.Cantidad AS FLOAT64) > 0 
                                                              THEN CAST(s.Cantidad AS FLOAT64) ELSE 1 END)
                ELSE 0 
            END AS ticket_promedio
        FROM `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_VENTAS_ID}` s
        JOIN `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_NEGOCIOS_ID}` n
            ON s.CodigoNegocio = n.CodigoNegocio
        WHERE (n.Descripcion = '{restaurante}' OR LOWER(n.Descripcion) LIKE '%{restaurante.lower()}%')
            AND Estado = '0.0'
            AND EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = {anio}
            AND s.Monto IS NOT NULL
            AND CAST(s.Monto AS FLOAT64) > 0
        GROUP BY n.Descripcion, EXTRACT(YEAR FROM DATE(s.FechaIntegrada)), EXTRACT(MONTH FROM DATE(s.FechaIntegrada))
        ORDER BY mes
    '''
    
    df = bigquery_client.query(query).to_dataframe()
      # Mostrar resumen detallado
    if not df.empty:
        restaurante_encontrado = df['restaurante'].iloc[0]
        total_registros = df['total_registros'].sum()
        total_transacciones = df['total_transacciones'].sum()
        print(f"Datos obtenidos para '{restaurante_encontrado}' - {anio}: {len(df)} meses")
        print(f"   {total_registros} registros en BD → {total_transacciones} transacciones calculadas")
        if restaurante_encontrado != restaurante:
            print(f"   📝 Nota: Se usó '{restaurante_encontrado}' (coincidencia parcial)")
    else:
        print(f"No se encontraron datos para '{restaurante}' en {anio}")
        
        # Intentar con una búsqueda más amplia solo para el año de comparación
        if anio == ANIO_COMPARACION_MENSUAL:
            print(f"🔄 Intentando búsqueda alternativa...")
            query_alternativa = f'''
                SELECT
                    n.Descripcion AS restaurante,
                    EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) AS anio,
                    EXTRACT(MONTH FROM DATE(s.FechaIntegrada)) AS mes,
                    COUNT(*) AS total_registros,
                    SUM(CAST(s.Monto AS FLOAT64)) AS total_ventas,
                    SUM(CASE WHEN s.Cantidad IS NOT NULL AND CAST(s.Cantidad AS FLOAT64) > 0 
                        THEN CAST(s.Cantidad AS FLOAT64) ELSE 1 END) AS total_transacciones,
                    -- Ticket promedio CORREGIDO
                    CASE 
                        WHEN SUM(CASE WHEN s.Cantidad IS NOT NULL AND CAST(s.Cantidad AS FLOAT64) > 0 
                                 THEN CAST(s.Cantidad AS FLOAT64) ELSE 1 END) > 0 
                        THEN SUM(CAST(s.Monto AS FLOAT64)) / SUM(CASE WHEN s.Cantidad IS NOT NULL AND CAST(s.Cantidad AS FLOAT64) > 0 
                                                                      THEN CAST(s.Cantidad AS FLOAT64) ELSE 1 END)
                        ELSE 0 
                    END AS ticket_promedio
                FROM `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_VENTAS_ID}` s
                JOIN `{PROJECT_ID}.{DATASET_VENTAS_ID}.{TABLE_NEGOCIOS_ID}` n
                    ON s.CodigoNegocio = n.CodigoNegocio
                WHERE EXTRACT(YEAR FROM DATE(s.FechaIntegrada)) = {anio}
                    AND Estado = '0.0'
                    AND s.Monto IS NOT NULL
                    AND CAST(s.Monto AS FLOAT64) > 0
                GROUP BY n.Descripcion, EXTRACT(YEAR FROM DATE(s.FechaIntegrada)), EXTRACT(MONTH FROM DATE(s.FechaIntegrada))
                HAVING COUNT(*) > 100  -- Solo restaurantes con datos significativos
                ORDER BY SUM(CAST(s.Monto AS FLOAT64)) DESC
                LIMIT 50  -- Top 50 por ventas
            '''
            df_alternativa = bigquery_client.query(query_alternativa).to_dataframe()
            if not df_alternativa.empty:
                # Usar el restaurante con más ventas como referencia
                restaurante_ref = df_alternativa['restaurante'].iloc[0]
                df = df_alternativa[df_alternativa['restaurante'] == restaurante_ref].copy()
                print(f"Usando datos de '{restaurante_ref}' como referencia del {anio}")
    
    return df


def crear_tablas_comparativas_anuales(df_actual, df_anterior):
    """Crea tablas comparativas de ventas y ticket promedio por mes"""
    
    # Solo mostrar hasta el mes actual del año actual
    mes_limite = MES_ACTUAL  # Usar la variable global del mes actual
    meses_completos = list(range(1, mes_limite + 1))
    nombres_meses = [month_map[mes] for mes in meses_completos]
    
    # Preparar datos de ventas
    ventas_actuales = []
    ventas_anteriores = []
    tickets_actuales = []
    tickets_anteriores = []
    
    for mes in meses_completos:
        # Ventas actuales
        venta_actual = 0
        ticket_actual = 0
        if not df_actual.empty:
            mes_data = df_actual[df_actual['mes'] == mes]
            if not mes_data.empty:
                venta_actual = mes_data['total_ventas'].iloc[0]
                ticket_actual = mes_data['ticket_promedio'].iloc[0]
        
        # Ventas anteriores
        venta_anterior = 0
        ticket_anterior = 0
        if not df_anterior.empty:
            mes_data = df_anterior[df_anterior['mes'] == mes]
            if not mes_data.empty:
                venta_anterior = mes_data['total_ventas'].iloc[0]
                ticket_anterior = mes_data['ticket_promedio'].iloc[0]
        
        ventas_actuales.append(venta_actual)
        ventas_anteriores.append(venta_anterior)
        tickets_actuales.append(ticket_actual)
        tickets_anteriores.append(ticket_anterior)
    
    # LÓGICA CORREGIDA: Si no hay ventas en año anterior (locatario no existía), diferencia = 0
    # Este enfoque garantiza una comparación justa para los locatarios que no operaban el año anterior
    def calcular_variaciones_corregidas(actuales, anteriores):
        diferencias = []
        diferencias_pct = []
        
        for actual, anterior in zip(actuales, anteriores):
            # Si no hay ventas en el año anterior, diferencia = 0
            if anterior == 0:
                diferencias.append(0)
                diferencias_pct.append(0)
            else:
                # Cálculo normal cuando hay datos del año anterior
                diferencia = actual - anterior
                diferencias.append(diferencia)
                pct = (diferencia / anterior) * 100
                diferencias_pct.append(pct)
        
        return diferencias, diferencias_pct
    
    # Tabla de ventas con lógica corregida
    dif_ventas, dif_ventas_pct = calcular_variaciones_corregidas(ventas_actuales, ventas_anteriores)
    
    tabla_ventas = pd.DataFrame({
        'Mes': nombres_meses,
        'Actual': [f"S/ {v:,.0f}" for v in ventas_actuales],
        'Año Anterior': [f"S/ {v:,.0f}" for v in ventas_anteriores],
        'Diferencia': [f"S/ {d:+,.0f}" for d in dif_ventas],
        'Diferencia %': [f"{p:+.1f}%" for p in dif_ventas_pct],
        'Actual_Num': ventas_actuales,  # Para gráficos
        'Anterior_Num': ventas_anteriores,
        'Diferencia_Num': dif_ventas,
        'Diferencia_Pct_Num': dif_ventas_pct
    })
    
    # Tabla de tickets con lógica corregida
    dif_tickets, dif_tickets_pct = calcular_variaciones_corregidas(tickets_actuales, tickets_anteriores)
    
    tabla_tickets = pd.DataFrame({
        'Mes': nombres_meses,
        'Actual': [f"S/ {t:.2f}" for t in tickets_actuales],
        'Año Anterior': [f"S/ {t:.2f}" for t in tickets_anteriores],
        'Diferencia': [f"S/ {d:+.2f}" for d in dif_tickets],
        'Diferencia %': [f"{p:+.1f}%" for p in dif_tickets_pct],
        'Actual_Num': tickets_actuales,  # Para gráficos
        'Anterior_Num': tickets_anteriores,
        'Diferencia_Num': dif_tickets,
        'Diferencia_Pct_Num': dif_tickets_pct
    })
    
    # DIAGNÓSTICO: Mostrar meses sin datos del año anterior
    meses_sin_datos_anteriores = [i+1 for i, v in enumerate(ventas_anteriores) if v == 0]
    if meses_sin_datos_anteriores:
        nombres_meses_sin_datos = [month_map[mes] for mes in meses_sin_datos_anteriores]
        print(f"📋 DIAGNÓSTICO para comparación mensual:")
        print(f"   - Meses sin ventas en {ANIO_COMPARACION_MENSUAL}: {len(meses_sin_datos_anteriores)} meses")
        print(f"   - Meses: {', '.join(nombres_meses_sin_datos)}")
        print(f"   - Diferencias ajustadas a 0 (locatario no existía)")
    else:
        print(f"✅ Datos completos en {ANIO_COMPARACION_MENSUAL} para todos los meses analizados")
    
    return tabla_ventas, tabla_tickets, meses_sin_datos_anteriores


def crear_graficos_comparativos_anuales(tabla_ventas, tabla_tickets, restaurante):

    import matplotlib.pyplot as plt
    import os
    
    # Solo mostrar hasta el mes actual
    mes_limite = MES_ACTUAL
    meses_num = list(range(1, mes_limite + 1))
    nombres_meses_cortos = [m[:3] for m in tabla_ventas['Mes'][:mes_limite]]
    
    # Determinar si hay meses sin datos para mostrar la nota explicativa
    tiene_meses_sin_datos = any(tabla_ventas['Anterior_Num'][:mes_limite] == 0)
    
    # Layout adaptativo según necesidad
    if tiene_meses_sin_datos:
        # Con espacio para nota: 55% gráfico, 10% nota, 35% tabla
        fig = plt.figure(figsize=(22, 18))
        gs = fig.add_gridspec(3, 1, height_ratios=[1, 0.1, 0.7], hspace=0.15)
        
    else:
        # Sin nota: 70% gráfico, 30% tabla
        fig = plt.figure(figsize=(22, 18))
        gs = fig.add_gridspec(2, 1, height_ratios=[1, 0.7], hspace=0.15)

    # Gráfico de ventas - solo hasta mes actual
    ax1 = fig.add_subplot(gs[0])
    ventas_actual_limitado = tabla_ventas['Actual_Num'][:mes_limite]
    ventas_anterior_limitado = tabla_ventas['Anterior_Num'][:mes_limite]
    
    ax1.plot(meses_num, ventas_actual_limitado, marker='o', linewidth=3, markersize=8,
             label=f'Ventas {ANIO_ACTUAL_MENSUAL}', color='#4CAF50', markerfacecolor='white', markeredgewidth=2)
    ax1.plot(meses_num, ventas_anterior_limitado, marker='s', linewidth=3, markersize=8,
             label=f'Ventas {ANIO_COMPARACION_MENSUAL}', color='#FF9800', markerfacecolor='white', markeredgewidth=2)
    
    ax1.set_title(f'Evolución de Ventas Mensuales - {restaurante}\n(Enero - {month_map[mes_limite]} {ANIO_ACTUAL_MENSUAL} vs {ANIO_COMPARACION_MENSUAL})', 
                  fontsize=16, fontweight='bold', pad = 20)    
    ax1.set_xticks(meses_num)
    ax1.set_xticklabels(nombres_meses_cortos)
    ax1.set_xlabel('Meses del año', fontsize=12)
    ax1.set_ylabel('Ventas (S/)', fontsize=12)
    ax1.legend(fontsize=12)
    ax1.grid(True, alpha=0.3)
    
    # Formatear eje Y con separadores de miles
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'S/ {x:,.0f}'))
    
    # Agregar valores en los puntos para mejor visualización
    for i, (mes, valor) in enumerate(zip(meses_num, ventas_actual_limitado)):
        if valor > 0:
            ax1.annotate(f'S/ {valor:,.0f}', 
                        (mes, valor), 
                        textcoords="offset points", 
                        xytext=(0,10), 
                        ha='center', fontsize=9, fontweight='bold',
                        color='#2E7D32')



    # === NOTA EXPLICATIVA ENTRE GRÁFICO Y TABLA ===
    # Crear área para la nota si hay meses sin datos
    if tiene_meses_sin_datos:
        # Agregar la nota explicativa
        ax_nota = fig.add_subplot(gs[1])
        ax_nota.axis('off')  # Sin ejes para la nota
        
        # Crear el texto de la nota
        meses_sin_datos_anteriores = [i+1 for i, v in enumerate(tabla_ventas['Anterior_Num'][:mes_limite]) if v == 0]
        nombres_meses_sin_datos = [month_map[mes] for mes in meses_sin_datos_anteriores]
        meses_sin_datos_count = len(meses_sin_datos_anteriores)
        
        nota_texto = (f"NOTA IMPORTANTE: {restaurante} presenta {meses_sin_datos_count} meses con diferencias = 0 "
                     f"debido a que no tuvo operaciones durante esas fechas.")
        
        # Mostrar la nota con buen formato
        ax_nota.text(0.5, 0.0, nota_texto, transform=ax_nota.transAxes,
                    fontsize=12, fontweight='bold', ha='center', va='center',
                    bbox=dict(boxstyle="round,pad=0.4", facecolor="lightyellow", 
                              alpha=0.95, edgecolor='orange', linewidth=1),
                    wrap=True)
        
        # Configurar el subplot para la tabla
        ax_tabla = fig.add_subplot(gs[2])
    else:
        # Si no hay meses sin datos, usar el layout original
        ax_tabla = fig.add_subplot(gs[1])

    # Tabla de ventas mejorada - solo hasta mes actual
    ax_tabla.axis('off')
    tabla_ventas_limitada = tabla_ventas[:mes_limite]
    tabla_ventas_display = tabla_ventas_limitada[['Mes', 'Actual', 'Año Anterior', 'Diferencia', 'Diferencia %']]
    
    table1 = ax_tabla.table(cellText=tabla_ventas_display.values,
                      colLabels=tabla_ventas_display.columns,
                      cellLoc='center', loc='center', bbox=[0, 0, 1, 1])
    
    table1.auto_set_font_size(False)
    table1.set_fontsize(10)
    table1.scale(1, 2)
    
    # Colorear encabezados
    for i in range(len(tabla_ventas_display.columns)):
        table1[(0, i)].set_facecolor('#1976D2')
        table1[(0, i)].set_text_props(weight='bold', color='white')
    
    # Colorear celdas según diferencias
    for i in range(1, len(tabla_ventas_limitada) + 1):
        for j in range(len(tabla_ventas_display.columns)):
            if i % 2 == 0:
                table1[(i, j)].set_facecolor('#f8f9fa')
            
            # Colorear diferencias según la lógica corregida
            if j == 3:  # Columna de Diferencia
                diff_num = tabla_ventas_limitada.iloc[i-1]['Diferencia_Num']
                if diff_num == 0:
                    table1[(i, j)].set_facecolor('#f0f0f0')
                    table1[(i, j)].set_text_props(color='black', weight='normal')
                elif diff_num > 0:
                    table1[(i, j)].set_facecolor('#e8f5e8')
                    table1[(i, j)].set_text_props(color='#2e7d32', weight='bold')
                elif diff_num < 0:
                    table1[(i, j)].set_facecolor('#ffebee')
                    table1[(i, j)].set_text_props(color='#d32f2f', weight='bold')
            
            # Colorear diferencias porcentuales
            if j == 4:  # Columna de Diferencia %
                diff_pct = tabla_ventas_limitada.iloc[i-1]['Diferencia_Pct_Num']
                if diff_pct == 0:
                    table1[(i, j)].set_facecolor('#f0f0f0')
                    table1[(i, j)].set_text_props(color='black', weight='normal')
                elif diff_pct > 0:
                    table1[(i, j)].set_facecolor('#e8f5e8')
                    table1[(i, j)].set_text_props(color='#2e7d32', weight='bold')
                elif diff_pct < 0:
                    table1[(i, j)].set_facecolor('#ffebee')
                    table1[(i, j)].set_text_props(color='#d32f2f', weight='bold')
    
    # ax_tabla.set_title('Tabla de Ventas Mensuales (Comparativo)', fontsize=14, fontweight='bold', pad=15)

    # Tabla de ticket promedio mejorada - solo hasta mes actual
    # ax4 = fig.add_subplot(gs[3])
    # ax4.axis('off')
    # tabla_tickets_limitada = tabla_tickets[:mes_limite]
    # tabla_tickets_display = tabla_tickets_limitada[['Mes', 'Actual', 'Año Anterior', 'Diferencia', 'Diferencia %']]
    
    # table2 = ax4.table(cellText=tabla_tickets_display.values,
    #                   colLabels=tabla_tickets_display.columns,
    #                   cellLoc='center', loc='center', bbox=[0, 0, 1, 1])
    # table2.auto_set_font_size(False)
    # table2.set_fontsize(10)
    # table2.scale(1, 1.5)
    
    # Colorear encabezados
    # for i in range(len(tabla_tickets_display.columns)):
    #     table2[(0, i)].set_facecolor('#1976D2')
    #     table2[(0, i)].set_text_props(weight='bold', color='white')
    
    # Colorear celdas según diferencias
    # for i in range(1, len(tabla_tickets_limitada) + 1):
    #     for j in range(len(tabla_tickets_display.columns)):
    #         if i % 2 == 0:
    #             table2[(i, j)].set_facecolor('#f8f9fa')
            
    #         # Colorear diferencias porcentuales
    #         if j == 4:  # Columna de Diferencia %
    #             diff_text = tabla_tickets_display.iloc[i-1]['Diferencia %']
    #             if '+' in str(diff_text):
    #                 table2[(i, j)].set_facecolor('#e8f5e8')
    #                 table2[(i, j)].set_text_props(color='#2e7d32', weight='bold')
    #             elif '-' in str(diff_text):
    #                 table2[(i, j)].set_facecolor('#ffebee')
    #                 table2[(i, j)].set_text_props(color='#d32f2f', weight='bold')
    
    # ax4.set_title('Tabla de Ticket Promedio Mensual (Comparativo)', fontsize=14, fontweight='bold', pad=15)

    # plt.suptitle(f'Análisis Anual Comparativo - {restaurante}\n(Período: Enero - {month_map[mes_limite]} | {ANIO_ACTUAL_MENSUAL} vs {ANIO_COMPARACION_MENSUAL})', 
    #              fontsize=18, fontweight='bold', y=0.98)
    
    # filename = f'reportes4/analisis_anual_comparativo_{restaurante.lower().replace(" ", "_")}_{ANIO_ACTUAL_MENSUAL}_vs_{ANIO_COMPARACION_MENSUAL}.png'
    filename = f'{OUTPUT_FOLDER}/analisis_anual_comparativo_{restaurante.lower().replace(" ", "_")}_{ANIO_ACTUAL_MENSUAL}_vs_{ANIO_COMPARACION_MENSUAL}.png'
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.close(fig)
    print(f"\nGráfico anual guardado como: {filename}")
    
    # Mostrar resumen de datos verificados
    print(f"\nVERIFICACIÓN DE DATOS:")
    print(f"   - Período analizado: Enero - {month_map[mes_limite]} ({mes_limite} meses)")
    print(f"   - Total ventas {ANIO_ACTUAL_MENSUAL}: S/ {sum(tabla_ventas['Actual_Num'][:mes_limite]):,.0f}")
    print(f"   - Total ventas {ANIO_COMPARACION_MENSUAL}: S/ {sum(tabla_ventas['Anterior_Num'][:mes_limite]):,.0f}")
    diferencia_total = sum(tabla_ventas['Actual_Num'][:mes_limite]) - sum(tabla_ventas['Anterior_Num'][:mes_limite])
    print(f"   - Diferencia total: S/ {diferencia_total:+,.0f}")
    
    return


def generar_analisis_anual_comparativo(restaurante):
    """Función principal para generar el análisis anual comparativo"""
    print(f"\n=== ANÁLISIS ANUAL COMPARATIVO ===")
    print(f"Comparando: {ANIO_ACTUAL_MENSUAL} vs {ANIO_COMPARACION_MENSUAL}")
    print(f"Restaurante: {restaurante}")
    
    # Obtener datos anuales
    print(f"\nObteniendo datos de {ANIO_ACTUAL_MENSUAL}...")
    df_actual = obtener_datos_anuales_por_mes(restaurante, ANIO_ACTUAL_MENSUAL)
    
    print(f"Obteniendo datos de {ANIO_COMPARACION_MENSUAL}...")
    df_anterior = obtener_datos_anuales_por_mes(restaurante, ANIO_COMPARACION_MENSUAL)
    
    if df_actual.empty and df_anterior.empty:
        print(f"No se encontraron datos para {restaurante} en ninguno de los años")
        return None, None
    
    # Crear tablas comparativas
    tabla_ventas, tabla_tickets, meses_sin_datos = crear_tablas_comparativas_anuales(df_actual, df_anterior)
    
    # Mostrar resumen en consola
    print(f"\n📈 RESUMEN DE VENTAS MENSUALES:")
    print("=" * 80)
    print(tabla_ventas[['Mes', 'Actual', 'Año Anterior', 'Diferencia %']].to_string(index=False))
    
    print(f"\n🎯 RESUMEN DE TICKET PROMEDIO:")
    print("=" * 80)
    print(tabla_tickets[['Mes', 'Actual', 'Año Anterior', 'Diferencia %']].to_string(index=False))
    
    # Crear gráficos
    crear_graficos_comparativos_anuales(tabla_ventas, tabla_tickets, restaurante)
    
    # === ESTADÍSTICAS RESUMIDAS CON LÓGICA CORREGIDA ===
    total_actual = sum(tabla_ventas['Actual_Num'][:MES_ACTUAL])
    total_anterior = sum(tabla_ventas['Anterior_Num'][:MES_ACTUAL])
    
    # Aplicar la misma lógica: si no hay datos del año anterior, diferencia = 0
    if total_anterior == 0:
        diferencia_total = 0
        porcentaje_total = 0
        print(f"📊 ESTADÍSTICAS AJUSTADAS: {restaurante} no tenía operaciones en {ANIO_COMPARACION_MENSUAL}")
    else:
        diferencia_total = total_actual - total_anterior
        porcentaje_total = (diferencia_total / total_anterior) * 100
        print(f"📊 ESTADÍSTICAS NORMALES: Comparación válida entre {ANIO_ACTUAL_MENSUAL} y {ANIO_COMPARACION_MENSUAL}")
    
    print(f"\n💰 RESUMEN EJECUTIVO:")
    print(f"   - Total ventas {ANIO_ACTUAL_MENSUAL}: S/ {total_actual:,.0f}")
    print(f"   - Total ventas {ANIO_COMPARACION_MENSUAL}: S/ {total_anterior:,.0f}")
    print(f"   - Diferencia: S/ {diferencia_total:+,.0f} ({porcentaje_total:+.1f}%)")
    
    # Solo exportar imágenes, no CSV
    print(f"\nAnálisis completado - Solo se exportaron las imágenes")
    
    return tabla_ventas, tabla_tickets


if __name__ == "__main__":
    
    # Modo de ejecución - cambie según necesite
    MODO = "todos"  # Opciones: "todos", "individual"
    
    if MODO == "todos":
        # Generar análisis para todos los locatarios
        for locatario in locatarios_map:
            print(f"\n📊 Iniciando análisis para: {locatario}")
            generar_analisis_anual_comparativo(locatario)
    else:
        # Generar análisis para un locatario individual
        locatario = 'Bar Refugio'  # Cambie esto al locatario deseado
        print(f"\n📊 Iniciando análisis para: {locatario}")
        generar_analisis_anual_comparativo(locatario)
        
    print("\n" + "="*60)
    print("ANÁLISIS COMPLETADO")
    print("Revisa la carpeta 'reportes4' para ver los gráficos de líneas y tablas generados")
    print("="*60)
    
    
    
