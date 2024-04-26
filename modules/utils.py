import re
import os
import pandas as pd
from ydata_profiling import ProfileReport

def data_profiling(df, df_name, output_file=None):

    if os.path.exists(output_file) == False:
        os.makedirs(output_file)

    title = "Profiling Report - " + df_name
    filename = output_file + df_name + "_report.html"

    # criar relatório
    profile = ProfileReport(df, title= title) 

    # salvar resultados em um arquivo
    profile.to_file(filename)




def process_dataframe(df, df_name):

    print('Analisando a tabela ' + df_name + '\n')

    # Remover colunas com valores ausentes

    cols_df = df.columns

    print("Removendo colunas que possuem 100% de valores faltantes ...")
    df_cln = df.dropna(axis=1, how='all')

    cols_df_cln = df_cln.columns
    cols_removed = list(set(cols_df) - set(cols_df_cln))

    print('Colunas removidas da tabela ', df_name, ': \n', cols_removed)


    # Remover campos duplicados
    df_len = len(df_cln)

    print("\nRemovendo dados duplicados ...")

    df_cln = df_cln.drop_duplicates()

    dup_lines = len(df_cln) - df_len

    print('Foram removidas ', dup_lines, 'linhas da tabela ' + df_name)


    # Remover colunas constantes (opcional)
    print("\nRemovendo colunas constantes ...")

    list_constant = [col for col in df_cln.columns if df_cln[col].nunique() == 1]
    df_cln = df_cln.drop(list_constant, axis=1)

    print('Colunas constantes removidas da tabela ' + df_name + ': \n', list_constant)

    return df_cln    




def remove_outliers(df, cols_list):
    """
    Remover outliers de colunas específicas. Considera-se outliers em colunas numéricas 
    selecionadas com valor acima ou abaixo de 2 desvios padrão da média.

    Parameters
    ----------
    df: dataframe
        Dataframe processado.
        
    cols_list: list
        Lista de colunas para considerar a eliminação de outliers.

    Returns
    -------
    df: dataframe
        Dataframe sem outliers.

    """
    qtd_lines = len(df)

    df_aux = df.copy()
    for col in cols_list:
        low_limit = df_aux[col].quantile(.02) 
        high_limit = df_aux[col].quantile(.98) 

        df = df[(df[col]>low_limit) & (df[col]<high_limit)] 

    qtd_lines = qtd_lines - len(df)

    print("\nQuantidade de linhas (outliers) eliminadas: ", qtd_lines )

    return df

def remove_special_caracters(texto):
    if pd.isnull(texto):  # Verifica se o valor é nulo
        return None  # Retorna nulo se for o caso
    
    # Define a expressão regular para encontrar caracteres especiais
    padrao = r'[^a-zA-Z0-9\s.()]'  # Remove tudo que não é letra, número, espaço, ponto ou parênteses

    # Substitui os caracteres especiais por uma string vazia
    texto_limpo = re.sub(padrao, '', texto)
    
    return texto_limpo

def change_t_for_1(texto):
    if pd.isnull(texto):  # Verifica se o valor é nulo
        return "0"  # Retorna nulo se for o caso

    # Substitui todas as ocorrências de 't' por '1' e qualquer outro valor por '0'
    texto_substituido = ''.join(['1' if char.lower() == 't' else '0' for char in texto])
    
    return texto_substituido

def check_missing(df):
    res_missing = df.isna().sum()
    # res_missing = (res_missing/len(df))*100
    return res_missing[res_missing != 0]

def verificar_datas(df):

    ano_atual = pd.Timestamp.now().year

    # # Converter a coluna 'date' para o tipo datetime
    # df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # Verificar as condições
    condicao_dia = (df['date'].dt.day >= 1) & (df['date'].dt.day <= 31) # dias num intervalo de 1 a 31
    condicao_mes = (df['date'].dt.month >= 1) & (df['date'].dt.month <= 12) # meses num intervalo de 1 a 12
    condicao_ano = (df['date'].dt.year >= 1900) & (df['date'].dt.year <= ano_atual) # dias num intervalo de 1900 ao ano corrente

    # Aplicar as condições ao DataFrame
    df_filtrado = df[condicao_dia & condicao_mes & condicao_ano]

    # Verificar se há valores fora do intervalo
    valores_fora_intervalo = df[~(condicao_dia & condicao_mes & condicao_ano)]

    return df_filtrado, valores_fora_intervalo