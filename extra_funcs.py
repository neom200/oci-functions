import numpy as np
from datetime import datetime, timedelta
data_base = datetime(1899, 12, 30)

def mapa_substituir_cols_padrao(df):
    old_cols = list(df.columns.values)
    new_cols = list(df.iloc[0, :])
    return {old_cols[i]:new_cols[i] for i in range(len(old_cols))}

def mapa_substituir_cols(df, new_cols):
    old_cols = list(df.columns.values)
    return {old_cols[i]:new_cols[i] for i in range(len(old_cols)-1)}

def mapa_maker(old, new):
    return {old[i]:new[i] for i in range(len(old))}

def convert_datetime_array_to_date(times):
    dates = []
    for t in times:
        dates.append(t.strftime('%d/%m'))
    return np.array(dates)

def convert_datetime_rows_to_date(rows):
    dates = []
    for r in rows:
        dates.append(r.strftime('%d/%m/%Y'))
    return np.array(dates)

def converter_data(valor):
    convertido = data_base + timedelta(days=int(valor), hours=(valor % 1) * 24)
    return convertido.strftime('%d/%m/%Y')

def converter_str_to_datetime(s):
    return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')

def obter_nome_dia_semana(dia_semana):
    dias_semana = {
        0: 'Seg',
        1: 'Ter',
        2: 'Qua',
        3: 'Qui',
        4: 'Sex',
        5: 'Sáb',
        6: 'Dom'
    }
    return dias_semana.get(dia_semana)

def formatar_data(data):
    dia_semana = obter_nome_dia_semana(data.weekday())
    return f"{dia_semana}, {data.strftime('%d/%m')}"

def formatar_data_2(data):
    dia_semana = obter_nome_dia_semana(data.weekday())
    return f"{dia_semana},{data.strftime('%d/%m/%Y')}"
    
def transformar_colunas_dt_(date_cols):
    new_dates = np.array([formatar_data(t) for t in date_cols])
    times_map = {date_cols[i]:new_dates[i] for i in range(len(date_cols))}
    return times_map

def rename_date_columns(colunas_old):
    colunas_new = [formatar_data_2(c) for c in colunas_old]
    dates_map = {colunas_old[i]:colunas_new[i] for i in range(len(colunas_old))}
    return dates_map

#df.loc[:,df.columns.duplicated()].columns
def list_duplicates_of(seq,item):
    start_at = -1
    locs = []
    while True:
        try:
            loc = seq.index(item,start_at+1)
        except ValueError:
            break
        else:
            locs.append(loc)
            start_at = loc
    return locs

def change_new_cols(cols, duped):
    cols = cols.copy()
    for c in duped:
        idx = list_duplicates_of(cols, c)
        vals = list(range(1, len(idx)-1))
        for i in range(len(idx)):
            cols[idx[i]] = c+f"{vals[i]}"
    return cols

# ---- Functions to help for 3rd transformations
def create_columns_map(df):
    novas_cols = list(df.iloc[0, :])
    old_cols = list(df.columns.values)
    cols_map = {old_cols[i]:novas_cols[i] for i in range(len(old_cols))}
    return cols_map

def juntar_nomes(c1, c2):
    nc = []
    c3 = c1+c2
    for i in range(len(c3)):
        if c3[i] is not np.nan:
            nc.append(c3[i])
    return nc

def arranjar_colunas_juntar_nome(df):
    old_cols = list(df.columns.values)
    cols1 = list(df.iloc[1, :])
    cols2 = list(df.iloc[2, :])
    novas = juntar_nomes(cols1, cols2)
    cols_map = {old_cols[i]: novas[i] for i in range(len(old_cols))}
    return df.rename(columns=cols_map).iloc[2:, :]

def transpose_dataframe(df):
    df = df.transpose().reset_index(drop=True).copy()
    novas_cols = list(df.iloc[0, :])
    old_cols = list(df.columns.values)
    cols_map = {old_cols[i]:novas_cols[i] for i in range(len(old_cols))}
    return df.rename(columns=cols_map).iloc[1:, :]

