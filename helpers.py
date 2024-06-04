#import numpy as np
import pandas as pd
from oci.config import from_file
from oci.object_storage import ObjectStorageClient
import oci
import ads
from ads.common.auth import default_signer
from extra_funcs import *

sheet_names = ['PISO_ICB',
 'TRENS',
 'PISO_TRO',
 'DP_DRB',
 'NV_BTP',
 'NV_DPW',
 'Planilha4',
 'Planilha3',
 'Detalhes1',
 'Planilha5',
 'MAPA',
 'NV_PCZ',
 'REGRAS DE NEGÓCIO',
 'CHEIOS2',
 'Planilha2',
 'DASH_MD',
 'CR_PLUMA (2)',
 'Trens Descida',
 'Planilha1',
 'Planilha6',
 'STATUS REPROGRAMAÇÃO',
 'PG_MAPA_RONDO',
 'TRENS FUTUROS.',
 'CHEIOS',
 'ZENDESK',
 'Mult',
 'GHT',
 'VAZIOS',
 'ROTEIRIZAÇÃO',
 'LEVANTES',
 'DB_G',
 'PG_CROSS',
 'PG_PLUMA',
 'JN_PLUMA',
 'DASH_PLUMA',
 'Reserva Amaggi',
 'MAPA_PLUMA',
 'PG_MD(INT)',
 'PG_MD(EXT)',
 'PG_MN(EXT)',
 'PG_GL(EXT)',
 'PG_OUT(EXT)',
 'PG_ADM',
 'PROGRAMA',
 'JN_ADM',
 'CX.SUSP',
 'BASE_EMP',
 'BASE',
 'BASE_MD',
 'DASH_M+1',
 'N_Plu_M+1',
 'DASH_CAPACIDADES',
 'GHT_2',
 'FERRO',
 'Ficha_TRO',
 'RESUMO PUMA',
 'Planilha7']

ads.set_auth(auth="api_key", oci_config_location="/home/lucas_souz/.oci/config", profile="DEFAULT")

def initialize_process():
    config = from_file(file_location="/home/lucas_souz/.oci/config")
    obj_client = ObjectStorageClient(config)
    namespace = "grqn05sriwg6"
    bucket = "RAW"
    main_file = "Plano_Produção_Mercado_Externo.xlsx"
    return obj_client, namespace, bucket, main_file

def load_sheets(namespace, bucket, main_file):
    all_sheets = {}
    for sheet in sheet_names:
        df = pd.read_excel(f"oci://{bucket}@{namespace}/{main_file}", sheet_name=sheet, storage_options=default_signer())

        df = df.dropna(how='all', axis='columns')
        df = df.dropna(how='all')

        all_sheets[sheet] = df

    return all_sheets

def first_transformation(all_sheets):
    planilhas_vazias = {}
    for sheet in all_sheets.keys():
        df = all_sheets[sheet].copy()
        if len(all_sheets[sheet]) > 0:
            planilhas_vazias[sheet] = df
    return planilhas_vazias

def second_transformation(planilhas_vazias):
    new_sheets = {}
    
    for sheet in planilhas_vazias.keys():
        df = planilhas_vazias[sheet].copy()
        
        if sheet == "PISO_TRO":
            df = df.iloc[:, :16]
        if sheet == "NV_BTP":
            df = df.iloc[4:-1]
        if sheet ==  "NV_PCZ":
            df = df.iloc[4:-1]
        if sheet == "REGRAS DE NEGÓCIO":
            df = df.iloc[3:, 7:]
        if sheet == "DASH_MD":
            df1 = df.iloc[:16]
            df2 = df.iloc[16:]
            new_sheets[sheet+"_1"] = df1.copy()
            new_sheets[sheet+"_2"] = df2.copy()
        if sheet == "STATUS REPROGRAMAÇÃO":
            df = df.iloc[:-2, :19]
        if sheet == "PG_MAPA_RONDO":
            df = df.iloc[1:, :26]
        if sheet == "TRENS FUTUROS.":
            df = df.iloc[4:, :41]
        if sheet == "CHEIOS":
            df = df.iloc[:-1, :52]
        if sheet == "ZENDESK":
            df = df.iloc[:, :-2]
        if sheet == "GHT":
            df = df.iloc[:, :6]
        if sheet == "VAZIOS":
            df = df.iloc[:, :-1]
        if sheet == "ROTEIRIZAÇÃO":
            df = df.iloc[1:, 6:]
        if sheet == "LEVANTES":
            df1 = df.iloc[6:, 1:]
            df2 = df.iloc[:7, 19:].transpose().reset_index().copy()
            new_sheets[sheet+"_A"] = df1.copy()
            new_sheets[sheet+"_B"] = df2.copy()
        if sheet == "DB_G":
            df1 = df.iloc[1:17, :5]
            df2 = df.iloc[20:, :9]
            df3 = df.iloc[1:17, 10:16]
            new_sheets[sheet+"_1"] = df1.copy()
            new_sheets[sheet+"_2"] = df2.copy()
            new_sheets[sheet+"_3"] = df3.copy()
        if sheet == "PG_CROSS":
            df1 = df.iloc[3:, :]
            df2 = df.iloc[:4, 20:].transpose().reset_index()
            new_sheets[sheet+"_A"] = df1.copy()
            new_sheets[sheet+"_B"] = df2.copy()
        if sheet == "PG_PLUMA":
            df1 = df.iloc[3:, 1:]
            df2 = df.iloc[:4, 25:].transpose().reset_index()
            new_sheets[sheet+"_A"] = df1.copy()
            new_sheets[sheet+"_B"] = df2.copy()
        if sheet == "JN_PLUMA":
            df1 = df.iloc[5:, :]
            df2 = df.iloc[:6, 10:].transpose().reset_index()
            new_sheets[sheet+"_A"] = df1.copy()
            new_sheets[sheet+"_B"] = df2.copy()
        if sheet == "DASH_PLUMA":
            df1 = df.iloc[:16]
            df2 = df.iloc[16:]
            new_sheets[sheet+"_1"] = df1.copy()
            new_sheets[sheet+"_2"] = df2.copy()
        if sheet == "MAPA_PLUMA":
            df = df.iloc[:, :4]
        if sheet == "PG_MD(INT)":
            df1 = df.iloc[3:, :]
            df2 = df.iloc[:4, 26+12:].transpose().reset_index()
            new_sheets[sheet+"_A"] = df1.copy()
            new_sheets[sheet+"_B"] = df2.copy()
        if sheet == "PG_MD(EXT)":
            df1 = df.iloc[4:, :]
            df2 = df.iloc[:5, 40:].transpose().reset_index()
            new_sheets[sheet+"_A"] = df1.copy()
            new_sheets[sheet+"_B"] = df2.copy()
        if sheet == "PG_MN(EXT)":
            df1 = df.iloc[4:-2, :]
            df2 = df.iloc[:5, 26+1:].transpose().reset_index().copy()
            new_sheets[sheet+"_A"] = df1.copy()
            new_sheets[sheet+"_B"] = df2.copy()
        if sheet == "PG_GL(EXT)":
            df1 = df.iloc[4:, :]
            df2 = df.iloc[:5, 34:].transpose().reset_index()
            new_sheets[sheet+"_A"] = df1.copy()
            new_sheets[sheet+"_B"] = df2.copy()
        if sheet == "PG_OUT(EXT)":
            df1 = df.iloc[4:-1, :]
            df2 = df.iloc[:5, 26+1:].transpose().reset_index().copy()
            new_sheets[sheet+"_A"] = df1.copy()
            new_sheets[sheet+"_B"] = df2.copy()
        if sheet == "PG_ADM":
            df1 = df.iloc[4:]
            df2 = df.iloc[:5, 41:].transpose().reset_index()
            new_sheets[sheet+"_A"] = df1.copy()
            new_sheets[sheet+"_B"] = df2.copy()
        if sheet == "JN_ADM":
            df = df.iloc[5:-1]
        if sheet == "CX.SUSP":
            df = df.iloc[1:]
        if sheet == "BASE_EMP":
            df = df.iloc[:, 1:-1]
        if sheet == "DASH_M+1":
            df1 = df.iloc[1:19, :-143]
            df2 = df.iloc[30:-1, :-143]
            new_sheets[sheet+"_1"] = df1.copy()
            new_sheets[sheet+"_2"] = df2.copy()
        if sheet == "N_Plu_M+1":
            df1 = df.iloc[:22, :7]
            df2 = df.iloc[23:38, :7]
            df3 = df.iloc[38:54, :7]
            df4 = df.iloc[54:-1, :7]
            new_sheets[sheet+"_1"] = df1.copy()
            new_sheets[sheet+"_2"] = df2.copy()
            new_sheets[sheet+"_3"] = df3.copy() 
            new_sheets[sheet+"_4"] = df4.copy() 
        if sheet == "Ficha_TRO":
            df1 = df.iloc[:9, :2]
            df2 = df.iloc[10:, :6]
            new_sheets[sheet+"_1"] = df1.copy()
            new_sheets[sheet+"_2"] = df2.copy()
        new_sheets[sheet] = df
    
    new_sheets.pop("DASH_MD")
    new_sheets.pop("DB_G")
    new_sheets.pop("JN_PLUMA")
    new_sheets.pop("LEVANTES")
    new_sheets.pop("PG_ADM")
    new_sheets.pop("PG_CROSS")
    new_sheets.pop("PG_GL(EXT)")
    new_sheets.pop("PG_MD(EXT)")
    new_sheets.pop("PG_MD(INT)")
    new_sheets.pop("PG_MN(EXT)")
    new_sheets.pop("PG_OUT(EXT)")
    new_sheets.pop("PG_PLUMA")
    new_sheets.pop("DASH_PLUMA")
    new_sheets.pop("DASH_M+1")
    new_sheets.pop("Ficha_TRO")
    new_sheets.pop("N_Plu_M+1")

    return new_sheets

def third_transformation(new_sheets):
    novas_sheets = {}
    
    for sheet in new_sheets.keys():
        df = new_sheets[sheet].copy()
        
        if sheet == "TRENS":
            df = transpose_dataframe(df)
        if sheet == "DP_DRB":
            cols_map = create_columns_map(df)
            df = df.rename(columns=cols_map).iloc[1:, :]
        if sheet == "NV_BTP":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "Detalhes1":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "NV_PCZ":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "REGRAS DE NEGÓCIO":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "DASH_MD_1":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "DASH_MD_2":
            old_cols = list(df.columns.values)
            novas1 = list(df.iloc[0, :])
            novas2 = list(df.iloc[1, :])
            novas = juntar_nomes(novas1, novas2)
            cols_map = {old_cols[i]: novas[i] for i in range(len(old_cols))}
            df = df.rename(columns=cols_map).iloc[2:, :]
        if sheet == "CR_PLUMA (2)":
            df = transpose_dataframe(df)
        if sheet == "Trens Descida":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, 1:]
        if sheet == "STATUS REPROGRAMAÇÃO":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "PG_MAPA_RONDO":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "TRENS FUTUROS.":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "GHT":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "VAZIOS":
            df = transpose_dataframe(df)
        if sheet == "ROTEIRIZAÇÃO":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "LEVANTES_A" or sheet == "LEVANTES_B":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "DB_G_1":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "DB_G_2":
            df = arranjar_colunas_juntar_nome(df)
        if sheet == "DB_G_3":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "PG_CROSS_A":
            df = df.rename(columns=mapa_substituir_cols_padrao(df)).iloc[2:, :]
        if sheet == "PG_CROSS_B":
            df = df.rename(columns=mapa_substituir_cols_padrao(df)).iloc[1:, :]
        if sheet == "PG_PLUMA_A" or sheet == "PG_PLUMA_B":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "JN_PLUMA_A" or sheet == "JN_PLUMA_B":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
            if sheet == "JN_PLUMA_A":
                df.columns.values[8] = "STATUS2"
            if sheet == "JN_PLUMA_B":
                df.columns.values[-2] = "DIA DA SEMANA"
        if sheet == "DASH_PLUMA_1":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "DASH_PLUMA_2":
            df = arranjar_colunas_juntar_nome(df)
        if sheet == "Reserva Amaggi":
            df = transpose_dataframe(df)
        if sheet == "PG_MD(INT)_A" or sheet == "PG_MD(INT)_B":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "PG_MD(EXT)_A":
            df = df.rename(columns=mapa_substituir_cols_padrao(df)).iloc[3:, :]
        if sheet == "PG_MD(EXT)_B":
            df = df.rename(columns=mapa_substituir_cols_padrao(df)).iloc[1:, :]
        if sheet == "PG_MN(EXT)_A" or sheet == "PG_MN(EXT)_B":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "PG_GL(EXT)_A":
            df = df.rename(columns=mapa_substituir_cols_padrao(df)).iloc[3:, :]
        if sheet == "PG_GL(EXT)_B":
            df = df.rename(columns=mapa_substituir_cols_padrao(df)).iloc[1:, :]
        if sheet == "PG_OUT(EXT)_A":
            df = df.rename(columns=create_columns_map(df)).iloc[2:, :]
        if sheet == "PG_OUT(EXT)_B":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "PG_ADM_A":
            df = df.rename(columns=mapa_substituir_cols_padrao(df)).iloc[2:, :]
        if sheet == "PG_ADM_B":
            df = df.rename(columns=mapa_substituir_cols_padrao(df)).iloc[1:, :]
        if sheet == "PROGRAMA":
            df = transpose_dataframe(df)
        if sheet == "JN_ADM":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "CX.SUSP":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "BASE_EMP":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "BASE":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "BASE_MD":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "DASH_M+1_1":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "DASH_M+1_2":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "N_Plu_M+1_1":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "N_Plu_M+1_2":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "N_Plu_M+1_3":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "N_Plu_M+1_4":
            df = df.rename(columns=create_columns_map(df)).iloc[1:, :]
        if sheet == "DASH_CAPACIDADES":
            df = transpose_dataframe(df)
        
        novas_sheets[sheet] = df

    return novas_sheets

def fourth_transformation(novas_sheets):
    novas_limpas = {}

    for sheet in novas_sheets.keys():
        df = novas_sheets[sheet].copy()
        df = df.dropna(how='all', axis='columns')
        df = df.dropna(how='all')
        novas_limpas[sheet] = df

    return novas_limpas

def extra_date_transformation(novas_sheets):
    novas_transformadas = {}
    for sheet in novas_sheets.keys():
        df = novas_sheets[sheet].copy()
    
        if sheet == "JN_PLUMA_A":
            old_cols = df.columns.values[11:]
            df = df.rename(columns=rename_date_columns(old_cols))
        if sheet == "LEVANTES_A":
            old_cols = list(df.columns.values[20:])
            df = df.rename(columns=rename_date_columns(old_cols))
        if sheet == "PG_ADM_A":
            old_cols = df.columns.values[42:]
            df = df.rename(columns=rename_date_columns(old_cols))
        if sheet == "PG_CROSS_A":
            old_cols = df.columns.values[21:]
            df = df.rename(columns=rename_date_columns(old_cols))
        if sheet == "PG_GL(EXT)_A":
            old_cols = df.columns.values[35:]
            df = df.rename(columns=rename_date_columns(old_cols))
        if sheet == "PG_MD(EXT)_A":
            old_cols = df.columns.values[41:]
            df = df.rename(columns=rename_date_columns(old_cols))
        if sheet == "PG_MD(INT)_A":
            old_cols = df.columns.values[26+13:]
            df = df.rename(columns=rename_date_columns(old_cols))
        if sheet == "PG_MN(EXT)_A":
            old_cols = df.columns.values[26+2:]
            df = df.rename(columns=rename_date_columns(old_cols))
        if sheet == "PG_OUT(EXT)_A":
            old_cols = df.columns.values[26+2:]
            df = df.rename(columns=rename_date_columns(old_cols))
        if sheet == "PG_PLUMA_A":
            old_cols = df.columns.values[25:]
            df = df.rename(columns=rename_date_columns(old_cols))
    
        novas_transformadas[sheet] = df

    return novas_transformadas

def fifth_transformation(novas_limpas):
    novas_no_dups = {}
    for sheet in novas_limpas.keys():
        df = novas_limpas[sheet].copy()

        if sheet == "DP_DRB":
            old_cols = list(df.columns.values)
            novas_cols = old_cols.copy()
            novas_cols[-1] = "COMPROV2"
            df = df.set_axis(novas_cols, axis=1)
        if sheet == "REGRAS DE NEGÓCIO":
            old_cols = list(df.columns.values)
            novas_cols = old_cols.copy()
            novas_cols[-8] = "Mapa2"
            df = df.set_axis(novas_cols, axis=1)
        if sheet == "GHT":
            old_cols = list(df.columns.values)
            novas_cols = old_cols.copy()
            novas_cols[-3] = "DATA2"
            novas_cols[-2] = "COR2"
            novas_cols[-1] = "TREM2"
            df = df.set_axis(novas_cols, axis=1)
        if sheet == "VAZIOS":
            #print(list(df.loc[:,df.columns.duplicated()].columns.values))
            old_cols = list(df.columns.values)
            novas_cols = old_cols.copy()
            novas_cols[21] = "BRADO2"
            novas_cols[31] = "BRADO3"
            novas_cols[36] = "BRADO4"
            novas_cols[45] = "BRADO5"
            novas_cols[22] = "CMA2"
            novas_cols[32] = "CMA3"
            novas_cols[37] = "CMA4"
            novas_cols[42] = "CMA5"
            novas_cols[24] = "ONE2"
            novas_cols[34] = "ONE3"
            novas_cols[39] = "ONE4"
            novas_cols[44] = "ONE5"
            novas_cols[25] = "BOOKADOS2"
            novas_cols[33] = "MAERSK2"
            novas_cols[38] = "MAERSK3"
            novas_cols[43] = "MAERSK4"
            novas_cols[35] = "MSC2"
            novas_cols[40] = "MSC3"
            novas_cols[46] = "MSC4"
            df = df.set_axis(novas_cols, axis=1)
        if sheet == "ROTEIRIZAÇÃO":
            #print(list(df.loc[:,df.columns.duplicated()].columns.values), '\n')
            old_cols = list(df.columns.values)
            novas_cols = old_cols.copy()
            novas_cols[40] = "BOOKING2"
            novas_cols[73] = "FUMIGAÇÃO \nDATA2"
            novas_cols[74] = "AERAÇÃO \nDATA2"
            novas_cols[82] = "CANAL2"
            novas_cols[93] = "TRANSFER \nDATA2"
            novas_cols[120] = "NONAME"
            novas_cols[121] = "NONAME2"
            novas_cols[122] = "NONAME3"
            novas_cols[123] = "NONAME4"
            novas_cols[124] = "NONAME5"
            df = df.set_axis(novas_cols, axis=1)
        if sheet == "JN_PLUMA":
            #print(list(df.loc[:,df.columns.duplicated()].columns.values), '\n')
            old_cols = list(df.columns.values)
            novas_cols = old_cols.copy()
            novas_cols[8] = "STATUS2"
            #print(list_duplicates_of(novas_cols, "STATUS"))
            #print(novas_cols)
            df = df.set_axis(novas_cols, axis=1)
        if sheet == "PROGRAMA":
            #print(list(df.loc[:,df.columns.duplicated()].columns.values), '\n')
            old_cols = list(df.columns.values)
            novas_cols = old_cols.copy()
            novas_cols[22] = "CLIENTE2"
            novas_cols[39] = "CLIENTE3"
            novas_cols[41] = "CLIENTE4"
            novas_cols[23] = "AGRO CRESTANI2"
            novas_cols[24] = "AMAGGI2"
            novas_cols[25] = "BOM JESUS2"
            novas_cols[26] = "BONGIOLO2"
            novas_cols[27] = "CARGILL2"
            novas_cols[28] = "MASUTTI2"
            novas_cols[29] = "SCHEFFER2"
            novas_cols[30] = "SLC_AGRICOLA2"
            novas_cols[31] = "VITERRA2"
            novas_cols[32] = "Capacidade (em ctnrs)2"
            novas_cols[33] = "Produtividade (em ctnrs)2"
            novas_cols[34] = "Realizado (em ctnrs)2"
            novas_cols[35] = "Programado (em cntrs)2"
            novas_cols[36] = "Capacidade disp. (em ctnrs)2"
            novas_cols[37] = "Total2"
            df = df.set_axis(novas_cols, axis=1)
        if sheet == "JN_ADM":
            #print(list(df.loc[:,df.columns.duplicated()].columns.values), '\n')
            old_cols = list(df.columns.values)
            novas_cols = old_cols.copy()
            novas_cols[8] = "STATUS2"
            #print(list_duplicates_of(novas_cols, "STATUS"))
            #print(novas_cols)
            df = df.set_axis(novas_cols, axis=1)
        if sheet == "CX.SUSP":
            #print(list(df.loc[:,df.columns.duplicated()].columns.values), '\n')
            old_cols = list(df.columns.values)
            novas_cols = old_cols.copy()
            novas_cols[24] = "MANIFESTO2"
            novas_cols[32] = "ARMADOR2"
            novas_cols[38] = "MÊS2"
            novas_cols[39] = "DESCRIÇÃO2"
            #print(list_duplicates_of(novas_cols, "DESCRIÇÃO"))
            #print(novas_cols)
            df = df.set_axis(novas_cols, axis=1)
        if sheet == "BASE_EMP":
            #print(list(df.loc[:,df.columns.duplicated()].columns.values), '\n')
            old_cols = list(df.columns.values)
            novas_cols = old_cols.copy()
            novas_cols[3] = "DATA2"
            novas_cols[5] = "DATA3"
            # print(list_duplicates_of(novas_cols, "DESCRIÇÃO"))
            df = df.set_axis(novas_cols, axis=1)
        if sheet == "BASE":
            #print(list(df.loc[:,df.columns.duplicated()].columns.values), '\n')
            old_cols = list(df.columns.values)
            novas_cols = old_cols.copy()
            novas_cols[8] = "Aceitos2"
            novas_cols[13] = "Aceitos3"
            novas_cols[14] = "Recusados2"
            novas_cols[15] = "Cancelados2"
            novas_cols[16] = "Pendentes2"
            novas_cols[22] = "Multimodal2"
            novas_cols[23] = "Round Trip2"
            novas_cols[24] = "Leasing2"
            df = df.set_axis(novas_cols, axis=1)
        if sheet == "BASE_MD":
            #print(list(df.loc[:,df.columns.duplicated()].columns.values), '\n')
            old_cols = list(df.columns.values)
            novas_cols = old_cols.copy()
            novas_cols[8] = "Aceitos2"
            novas_cols[13] = "Aceitos3"
            novas_cols[14] = "Recusados2"
            novas_cols[15] = "Cancelados2"
            novas_cols[16] = "Pendentes2"
            novas_cols[22] = "Multimodal2"
            novas_cols[23] = "Round Trip2"
            novas_cols[24] = "Leasing2"
            df = df.set_axis(novas_cols, axis=1)
        if sheet == "DASH_M+1_1":
            #print(list(df.loc[:,df.columns.duplicated()].columns.values), '\n')
            old_cols = list(df.columns.values)
            novas_cols = old_cols.copy()
            novas_cols[8] = "Total2"
            novas_cols[11] = "Aceitos2"
            novas_cols[12] = "Em Análise2"
            novas_cols[13] = "Recusados2"
            novas_cols[14] = "Cancelados2"
            #print(list_duplicates_of(novas_cols, "Cancelados"))
            df = df.set_axis(novas_cols, axis=1)
        if sheet == "N_Plu_M+1_2":
            #print(list(df.loc[:,df.columns.duplicated()].columns.values), '\n')
            old_cols = list(df.columns.values)
            novas_cols = old_cols.copy()
            novas_cols[4] = "Share(%)2"
            novas_cols[6] = "Share(%)3"
            #print(list_duplicates_of(novas_cols, "Share(%)"))
            df = df.set_axis(novas_cols, axis=1)
        if sheet == "N_Plu_M+1_3":
            #print(list(df.loc[:,df.columns.duplicated()].columns.values), '\n')
            old_cols = list(df.columns.values)
            novas_cols = old_cols.copy()
            novas_cols[6] = "Share(%)2"
            #print(list_duplicates_of(novas_cols, "Share(%)"))
            df = df.set_axis(novas_cols, axis=1)
        if sheet == "DASH_PLUMA_2":
            #print(list(df.loc[:,df.columns.duplicated()].columns.values), '\n')
            old_cols = list(df.columns.values)
            novas_cols = old_cols.copy()
            novas_cols[6] = "Aceitos2"
            novas_cols[7] = "Cancelados2"
            novas_cols[8] = "Recusados2"
            #print(list_duplicates_of(novas_cols, "Recusados"))
            df = df.set_axis(novas_cols, axis=1)
        if sheet == "DASH_MD_2":
            #print(list(df.loc[:,df.columns.duplicated()].columns.values), '\n')
            old_cols = list(df.columns.values)
            novas_cols = old_cols.copy()
            novas_cols[8] = "Recusados2"
            novas_cols[12] = "Recusados3"
            novas_cols[10] = "Aceitos2"
            novas_cols[11] = "Cancelados2"
            #print(list_duplicates_of(novas_cols, "Recusados"))
            df = df.set_axis(novas_cols, axis=1)
        if sheet == "CR_PLUMA (2)":
            #print(list(df.loc[:,df.columns.duplicated()].columns.values), '\n')
            old_cols = list(df.columns.values)
            novas_cols = old_cols.copy()
            novas_cols[7] = "OCUPAÇÃO (%)2"
            #print(list_duplicates_of(novas_cols, "OCUPAÇÃO (%)"))
            df = df.set_axis(novas_cols, axis=1)
        if sheet == "Trens Descida":
            #print(list(df.loc[:,df.columns.duplicated()].columns.values), '\n')
            old_cols = list(df.columns.values)
            novas_cols = old_cols.copy()
            novas_cols[7] = "Progr. Planner2"
            novas_cols[10] = "Progr. Planner3"
            novas_cols[13] = "Progr. Planner4"
            novas_cols[8] = "Progr. PCP2"
            novas_cols[11] = "Progr. PCP3"
            novas_cols[14] = "Progr. PCP4"
            novas_cols[9] = "Total2"
            novas_cols[12] = "Total3"
            novas_cols[15] = "Total4"
            novas_cols[17] = "Trem2"
            #print(list_duplicates_of(novas_cols, "Trem"))
            df = df.set_axis(novas_cols, axis=1)
            
        novas_no_dups[sheet] = df

    return novas_no_dups

def sixth_transformation(novas_no_dups):
    novos_dados = {}

    for sheet in novas_no_dups.keys():
        df = novas_no_dups[sheet].copy()
        colunas = list(df.columns.values)
        tipos = list(df.dtypes)
        for i in range(len(tipos)):
            if tipos[i] == 'object':
                df[colunas[i]] = df[colunas[i]].astype("string")
        novos_dados[sheet] = df

    return novos_dados

def extra_transformation(novos_dados):
    parquets_arrumar = ['PISO_TRO', 'ZENDESK', 'FERRO']
    for pq in parquets_arrumar:
        #df = pd.read_parquet(f'Parquet Files/{pq}.parquet')
        df = novos_dados[pq].copy()
        for col in df.columns:
            df[col] = df[col].astype('string')
        novos_dados[pq] = df
    return novos_dados

def seventh_transformation(novos_dados):
    parquets_sheets = {}
    for sheet in novos_dados.keys():
        parquets_sheets[sheet] = novos_dados[sheet].to_parquet()
    return parquets_sheets

def eight_transformation(parquets_sheets, obj_client):
    object_storage_namespace = "grqn05sriwg6"
    object_storage_bucket = "TRUSTED"

    for sheet in parquets_sheets.keys():
        obj_client.put_object(
            namespace_name=object_storage_namespace,
            bucket_name=object_storage_bucket,
            object_name=sheet,
            put_object_body=parquets_sheets[sheet]
        )
