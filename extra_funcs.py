import numpy as np

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