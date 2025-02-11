import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

import app.database.db as db
from config.settings import DATABASE_URL_TEST, DATABASE_URL

data = db.DatabaseManager(DATABASE_URL)

def dona_bls_por_state():
    # grafico tipo dona de bls por estado
    df = data.get_bls_reporte_rutina()
    df = pd.DataFrame(df)
    print(df)

    dict_navieras = data.get_navieras()
    dict_estados = data.get_dict_bl_state()

    dict_sts = {v['state_code']: v['state_description'] for v in dict_estados}
    dict_nav = {v['id']: v['nombre'] for v in dict_navieras}

    
    # Contar la frecuencia de cada estado
    estado_counts = df['state_code'].value_counts()
    # porcentaje de bls por estado
    estado_porcentaje = estado_counts / estado_counts.sum() * 100

    cargo_counts = pd.DataFrame({'Total': [df['id_carga'].value_counts().sum()]})

    naviera_detalle = df.groupby(['naviera_id', 'state_code']).size().unstack(fill_value=0)

    naviera_detalle = naviera_detalle.rename(columns=dict_sts)

    # Ordeno en una tabla la cantidad de bls de cada naviera que pertenezcan a un estado en especifico
    navieras_estados_table = df.groupby(['state_code', 'naviera_id']).size().unstack(fill_value=0)

    navieras_estados_table = navieras_estados_table.rename(index=dict_sts,columns=dict_nav)

    # Calcular la cantidad total de BLs por naviera
    total_bls = df.groupby('naviera_id').size()


    # Combinar la cantidad total con las cantidades por estado
    final_table = naviera_detalle.copy()

    final_table['Otro estado'] = final_table.drop('Toda la información descargada', axis=1).sum(axis=1)
    final_table['Total'] = total_bls

    #eliminar columnas distintas a 'Toda la información descargada'
    final_table = final_table[['Toda la información descargada', 'Otro estado', 'Total']]

    final_table.index = final_table.index.map(dict_nav)
    final_table = final_table.sort_values(by="Total", ascending=False)

    # cruzar ids con nombres
    estado_counts.index = estado_counts.index.map(dict_sts)
    estado_porcentaje.index = estado_porcentaje.index.map(dict_sts)
    # juntar estado_counts con estado_porcentaje
    estado_counts = pd.concat([estado_counts, estado_porcentaje], axis=1, keys=['Total', 'Porcentaje'])
    
    generar_grafico_estado(estado_counts)
    generar_grafico_naviera(final_table)

    estado_counts = estado_counts.drop('Porcentaje', axis=1)

    return estado_counts, final_table, cargo_counts, navieras_estados_table


def generar_grafico_naviera(data):
    sns.set_theme(style="whitegrid")

    # Initialize the matplotlib figure
    f, ax = plt.subplots()

    # Plot the total crashes
    sns.set_color_codes("pastel")
    sns.barplot(x="Total", y="naviera_id", data=data,
                label="Total de BLs", color="b")

    # Plot the crashes where alcohol was involved
    sns.set_color_codes("muted")
    sns.barplot(x="Toda la información descargada", y="naviera_id", data=data,
                label="Información descargada", color="b")

    # Add a legend and informative axis label
    ax.legend(ncol=2, loc="lower right", frameon=True)
    ax.set(ylabel="Navieras",
        xlabel="Cantidad de BLs")
    sns.despine(left=True, bottom=True)
    plt.tight_layout()
    plt.savefig('data/grafico_naviera.png', transparent=True)
    plt.close()
    

def generar_grafico_estado(data):
    sns.set_theme(style="whitegrid")

    # Initialize the matplotlib figure
    f, ax = plt.subplots()

    sns.set_color_codes("muted")
    ax = sns.barplot(x='Porcentaje', y='state_code', data=data, label="Información descargada", color="b", legend=False)

    # Añadir etiquetas de porcentaje en las barras
    for index, value in enumerate(data['Porcentaje']):
        plt.text(value, index, f'{value:.2f}%', va='center')

    ax.set(xlim=(0, 100), ylabel="",
        xlabel="Porcentaje de aparicion de un estado")

    sns.despine(left=True, bottom=True)
    plt.tight_layout()
    plt.savefig('data/grafico_estado.png', transparent=True)
    plt.close()


if __name__ == '__main__':
    dona_bls_por_state()