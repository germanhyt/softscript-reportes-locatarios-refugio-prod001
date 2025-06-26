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
DATASET_FLUJO_DE_PERSONAS_ID = 'flujo_de_personas'

# LOCAL
SERVICE_ACCOUNT_FILE = r'C:\Users\gcbso\Downloads\EMPRESA GCB\REFUGIO PROYECTO PYTHON LOCAL\neat-chain-450900-a1-f67bb083925b.json'
# N8N
# SERVICE_ACCOUNT_FILE = '/data/scripts/neat-chain-450900-a1-f67bb083925b.json'

# LOCAL PATH SAVE REPORTS
OUTPUT_FOLDER = 'reportes2'
# N8N PATH SAVE REPORTS
# OUTPUT_FOLDER = '/data/scripts/reportes2'


TABLE_VENTAS_ID = 'sales_df'
TABLE_CATEGORIAS_ID = 'categorias'
TABLE_METAS_ID = 'MontosMeta'
TABLE_NEGOCIOS_ID = 'Negocios'
TABLE_FLUJO_PERSONAS_ID = 'Total_Puertas_Hora'

# BigQuery client
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
bigquery_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

# === CONFIGURACIÓN DE AÑOS ===
ANIO_ACTUAL = 2025
ANIO_COMPARACION = 2024
MES_ACTUAL = 6  # Junio - mes hasta el cual comparar


def visualizar_tabla_flujo_de_personas():
    """Visualiza una muestra de la tabla de flujo de personas"""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_FLUJO_DE_PERSONAS_ID}.{TABLE_FLUJO_PERSONAS_ID}` LIMIT 10"
    try:
        df = bigquery_client.query(query).to_dataframe()
        print("Tabla 'Total_Puertas_Hora':")
        print(df.head())
        print(f"\nColumnas disponibles: {list(df.columns)}")
        print(f"Total de registros en muestra: {len(df)}")
    except Exception as e:
        print(f"Error al consultar la tabla 'Total_Puertas_Hora': {e}")


def obtener_datos_flujo_mensual(anio):
    """Obtiene datos de flujo de personas agrupados por mes para un año específico"""
    query = f'''
        SELECT
            EXTRACT(YEAR FROM DATE(Fecha)) AS anio,
            EXTRACT(MONTH FROM DATE(Fecha)) AS mes,
            SUM(CAST(Entradas AS INT64)) AS total_entradas,
            SUM(CAST(Salidas AS INT64)) AS total_salidas,
            COUNT(*) AS total_registros,
            COUNT(DISTINCT Puerta) AS total_puertas,
            COUNT(DISTINCT DATE(Fecha)) AS dias_con_datos
        FROM `{PROJECT_ID}.{DATASET_FLUJO_DE_PERSONAS_ID}.{TABLE_FLUJO_PERSONAS_ID}`
        WHERE EXTRACT(YEAR FROM DATE(Fecha)) = {anio}
            AND Entradas IS NOT NULL
            AND Salidas IS NOT NULL
            AND CAST(Entradas AS INT64) >= 0
            AND CAST(Salidas AS INT64) >= 0
        GROUP BY EXTRACT(YEAR FROM DATE(Fecha)), EXTRACT(MONTH FROM DATE(Fecha))
        ORDER BY mes
    '''
    
    df = bigquery_client.query(query).to_dataframe()
    
    if not df.empty:
        print(f"✅ Datos de flujo obtenidos para {anio}: {len(df)} meses")
        for _, row in df.iterrows():
            mes_nombre = month_map[int(row['mes'])]
            print(f"   {mes_nombre}: {row['total_entradas']:,} entradas | {row['total_salidas']:,} salidas | {row['dias_con_datos']} días")
    else:
        print(f"❌ No se encontraron datos de flujo para {anio}")
    
    return df


def crear_tabla_comparativa_flujo(df_actual, df_anterior):
    """Crea tabla comparativa de entradas mensuales"""
    
    # Solo mostrar hasta el mes actual
    mes_limite = MES_ACTUAL
    meses_completos = list(range(1, mes_limite + 1))
    nombres_meses = [month_map[mes] for mes in meses_completos]
    
    # Preparar datos de entradas
    entradas_actuales = []
    entradas_anteriores = []
    
    for mes in meses_completos:
        # Entradas año actual
        entrada_actual = 0
        if not df_actual.empty:
            mes_data = df_actual[df_actual['mes'] == mes]
            if not mes_data.empty:
                entrada_actual = int(mes_data['total_entradas'].iloc[0])
        
        # Entradas año anterior
        entrada_anterior = 0
        if not df_anterior.empty:
            mes_data = df_anterior[df_anterior['mes'] == mes]
            if not mes_data.empty:
                entrada_anterior = int(mes_data['total_entradas'].iloc[0])
        
        entradas_actuales.append(entrada_actual)
        entradas_anteriores.append(entrada_anterior)
    
    # Calcular diferencias y porcentajes
    diferencias = []
    diferencias_pct = []
    
    for actual, anterior in zip(entradas_actuales, entradas_anteriores):
        diferencia = actual - anterior
        diferencias.append(diferencia)
        
        if anterior != 0:
            pct = (diferencia / anterior) * 100
        else:
            pct = 0
        diferencias_pct.append(pct)
    
    # Crear tabla
    tabla_flujo = pd.DataFrame({
        'Mes': nombres_meses,
        'Entradas Actuales': [f"{e:,}" for e in entradas_actuales],
        'Entradas Año Anterior': [f"{e:,}" for e in entradas_anteriores],
        'Diferencia': [f"{d:+,}" for d in diferencias],
        'Diferencia %': [f"{p:+.1f}%" for p in diferencias_pct],
        'Actuales_Num': entradas_actuales,  # Para gráficos
        'Anteriores_Num': entradas_anteriores
    })
    
    return tabla_flujo


def crear_grafico_comparativo_flujo(tabla_flujo):
    """Crea gráfico comparativo de flujo de personas con tabla integrada"""
    
    # Solo mostrar hasta el mes actual
    mes_limite = MES_ACTUAL
    meses_num = list(range(1, mes_limite + 1))
    nombres_meses_cortos = [m[:3] for m in tabla_flujo['Mes'][:mes_limite]]
    
    fig = plt.figure(figsize=(18, 12))
    gs = fig.add_gridspec(2, 1, height_ratios=[2, 1], hspace=0.3)

    # === GRÁFICO DE LÍNEAS COMPARATIVO ===
    ax1 = fig.add_subplot(gs[0])
    
    entradas_actual_limitado = tabla_flujo['Actuales_Num'][:mes_limite]
    entradas_anterior_limitado = tabla_flujo['Anteriores_Num'][:mes_limite]
    
    # Líneas principales
    ax1.plot(meses_num, entradas_actual_limitado, marker='o', linewidth=3, markersize=8,
             label=f'Entradas {ANIO_ACTUAL}', color='#4CAF50', markerfacecolor='white', markeredgewidth=2)
    ax1.plot(meses_num, entradas_anterior_limitado, marker='s', linewidth=3, markersize=8,
             label=f'Entradas {ANIO_COMPARACION}', color='#FF9800', markerfacecolor='white', markeredgewidth=2)
    
    # Configuración del gráfico
    # ax1.set_title(f'Evolución Mensual de Entradas - Flujo de Personas\n(Enero - {month_map[mes_limite]} | {ANIO_ACTUAL} vs {ANIO_COMPARACION})', 
    #               fontsize=16, fontweight='bold', pad=20)
    ax1.set_xticks(meses_num)
    ax1.set_xticklabels(nombres_meses_cortos)
    ax1.set_ylabel('Número de Entradas', fontsize=12)
    ax1.legend(fontsize=12)
    ax1.grid(True, alpha=0.3)
    
    # Formatear eje Y con separadores de miles
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
    
    # Agregar valores en los puntos
    for i, (mes, valor) in enumerate(zip(meses_num, entradas_actual_limitado)):
        if valor > 0:
            ax1.annotate(f'{valor:,}', 
                        (mes, valor), 
                        textcoords="offset points", 
                        xytext=(0,10), 
                        ha='center', fontsize=9, fontweight='bold',
                        color='#2E7D32')
    
    for i, (mes, valor) in enumerate(zip(meses_num, entradas_anterior_limitado)):
        if valor > 0:
            ax1.annotate(f'{valor:,}', 
                        (mes, valor), 
                        textcoords="offset points", 
                        xytext=(0,-15), 
                        ha='center', fontsize=9, fontweight='bold',
                        color='#F57C00')

    # === TABLA COMPARATIVA ===
    ax2 = fig.add_subplot(gs[1])
    ax2.axis('off')
    
    tabla_limitada = tabla_flujo[:mes_limite]
    tabla_display = tabla_limitada[['Mes', 'Entradas Actuales', 'Entradas Año Anterior', 'Diferencia', 'Diferencia %']]
    
    table = ax2.table(cellText=tabla_display.values,
                     colLabels=tabla_display.columns,
                     cellLoc='center', loc='center', bbox=[0, 0, 1, 1])
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 2)
    
    # Colorear encabezados
    for i in range(len(tabla_display.columns)):
        table[(0, i)].set_facecolor('#1976D2')
        table[(0, i)].set_text_props(weight='bold', color='white')
    
    # Colorear celdas según diferencias
    for i in range(1, len(tabla_limitada) + 1):
        for j in range(len(tabla_display.columns)):
            if i % 2 == 0:
                table[(i, j)].set_facecolor('#f8f9fa')
            
            # Colorear diferencias porcentuales
            if j == 4:  # Columna de Diferencia %
                diff_text = tabla_display.iloc[i-1]['Diferencia %']
                if '+' in str(diff_text):
                    table[(i, j)].set_facecolor('#e8f5e8')
                    table[(i, j)].set_text_props(color='#2e7d32', weight='bold')
                elif '-' in str(diff_text):
                    table[(i, j)].set_facecolor('#ffebee')
                    table[(i, j)].set_text_props(color='#d32f2f', weight='bold')
    
    # Estadísticas de resumen
    total_actual = sum(tabla_flujo['Actuales_Num'][:mes_limite])
    total_anterior = sum(tabla_flujo['Anteriores_Num'][:mes_limite])
    diferencia_total = total_actual - total_anterior
    porcentaje_total = (diferencia_total / total_anterior * 100) if total_anterior != 0 else 0
    
    # Título con estadísticas
    plt.suptitle(f'Análisis Comparativo de Flujo de Personas\n'
                 f'Total {ANIO_ACTUAL}: {total_actual:,} entradas | Total {ANIO_COMPARACION}: {total_anterior:,} entradas | '
                 f'Diferencia: {porcentaje_total:+.1f}%', 
                 fontsize=14, fontweight='bold', y=0.95)
    
    # Guardar archivo
    # filename = f'reportes2/flujo_personas_comparativo_{ANIO_ACTUAL}_vs_{ANIO_COMPARACION}.png'
    filename = f'{OUTPUT_FOLDER}/flujo_personas_comparativo_{ANIO_ACTUAL}_vs_{ANIO_COMPARACION}.png'
    
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.close(fig)
    
    print(f"\n✅ Gráfico de flujo guardado como: {filename}")
    print(f"\n📊 RESUMEN DE FLUJO DE PERSONAS:")
    print(f"   - Total entradas {ANIO_ACTUAL}: {total_actual:,}")
    print(f"   - Total entradas {ANIO_COMPARACION}: {total_anterior:,}")
    print(f"   - Diferencia: {diferencia_total:+,} ({porcentaje_total:+.1f}%)")
    
    return tabla_flujo


def generar_reporte_flujo_personas():
    """Función principal para generar el reporte de flujo de personas"""
    print(f"\n=== ANÁLISIS DE FLUJO DE PERSONAS ===")
    print(f"Comparando: {ANIO_ACTUAL} vs {ANIO_COMPARACION}")
    print(f"Período: Enero - {month_map[MES_ACTUAL]}")
    
    # Obtener datos de ambos años
    print(f"\n📊 Obteniendo datos de flujo para {ANIO_ACTUAL}...")
    df_actual = obtener_datos_flujo_mensual(ANIO_ACTUAL)
    
    print(f"📊 Obteniendo datos de flujo para {ANIO_COMPARACION}...")
    df_anterior = obtener_datos_flujo_mensual(ANIO_COMPARACION)
    
    if df_actual.empty and df_anterior.empty:
        print(f"❌ No se encontraron datos de flujo para ninguno de los años")
        return None
    
    # Crear tabla comparativa
    tabla_flujo = crear_tabla_comparativa_flujo(df_actual, df_anterior)
    
    # Mostrar tabla en consola
    print(f"\n📈 TABLA COMPARATIVA DE ENTRADAS:")
    print("=" * 80)
    print(tabla_flujo[['Mes', 'Entradas Actuales', 'Entradas Año Anterior', 'Diferencia %']].to_string(index=False))
    
    # Crear gráfico
    crear_grafico_comparativo_flujo(tabla_flujo)
    
    print(f"\n✅ Reporte de flujo de personas completado")
    
    return tabla_flujo


# main
if __name__ == "__main__":
    print("=== GENERADOR DE REPORTES DE FLUJO DE PERSONAS ===")
    print(f"Análisis comparativo: {ANIO_ACTUAL} vs {ANIO_COMPARACION}")
    print(f"Período: Enero - {month_map[MES_ACTUAL]}")
    
    # print("\n" + "="*60)
    # print("1. VISUALIZACIÓN DE DATOS DE MUESTRA:")
    # print("="*60)
    
    # Visualizar muestra de la tabla
    visualizar_tabla_flujo_de_personas()
    
    # print("\n" + "="*60)
    # print("2. GENERANDO REPORTE COMPARATIVO:")
    # print("="*60)
    
    # Generar reporte completo
    generar_reporte_flujo_personas()
    
    print("\n" + "="*60)
    print("✅ ANÁLISIS FINALIZADO")
    # print("📁 Revisa la carpeta 'reportes2' para ver el gráfico generado")
    # print("="*60)