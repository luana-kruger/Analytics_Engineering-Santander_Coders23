import re
import os
from datetime import datetime
import pandas as pd
from ydata_profiling import ProfileReport
import great_expectations as gx


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

def check_date(df, col):

    ano_atual = pd.Timestamp.now().year
    
    # Verificar as condições
    condicao_dia = (df[col].dt.day >= 1) & (df[col].dt.day <= 31) # dias num intervalo de 1 a 31
    condicao_mes = (df[col].dt.month >= 1) & (df[col].dt.month <= 12) # meses num intervalo de 1 a 12
    condicao_ano = (df[col].dt.year >= 1900) & (df[col].dt.year <= ano_atual) # dias num intervalo de 1900 ao ano corrente

    # Aplicar as condições ao DataFrame
    df_filtrado = df[condicao_dia & condicao_mes & condicao_ano]

    # Verificar se há valores fora do intervalo
    valores_fora_intervalo = df[~(condicao_dia & condicao_mes & condicao_ano)]

    return df_filtrado, valores_fora_intervalo

'''
##########################################################################

OBS: a proxima parte é focada apenas para funções do Great Expectations

##########################################################################
'''

def clean_path(path:str):
    '''
    '''
    print(f"Listando os arquivos do diretório {path}")
    files_list = [os.path.join(path,file) for file in os.listdir(path=path)]
    
    print(f"Será apagados {len(files_list)} arquivos.")
    for file_path in files_list:
        print(f"ERRO: O arquivo {file_path} está sendo apagado.")
        os.remove(path=file_path)
        print(f"ERRO: Arquivo {file_path} apagado com sucesso!!")


def verifica_colunas_datetime(gx_df, colunas_datetime:list):
    for coluna in colunas_datetime:
        condicao_tipo = gx_df.expect_column_values_to_be_in_type_list(coluna, ['datetime64[ns]']).success
        condicao_data_valida = gx_df.expect_column_values_to_be_between(coluna, datetime(2008,8,1), datetime.now()).success
        if condicao_tipo and condicao_data_valida:
            print(f"A coluna '{coluna}' é válida. (Tipo e Valor)")
        else:
            if not condicao_tipo:
                print(f"ERRO: A coluna '{coluna}' não está no formato correto. Esperado: datetime.")
            if not condicao_data_valida:
                print(f"ERRO: A coluna '{coluna}' tem valores fora do intervalo permitido.")

def verificar_colunas_categoricas(gx_df, coluna_a_ser_analisada, valores_esperados):
    result = gx_df.expect_column_values_to_be_in_set(coluna_a_ser_analisada, valores_esperados)
    if result.success:
        print(f"Os valores da coluna {coluna_a_ser_analisada} contêm apenas valores esperados. ({valores_esperados})")
    else:
        print(f"ERRO: Os valores da coluna {coluna_a_ser_analisada} contêm valores inesperados. ({result.result['unexpected_list']})")


def verificar_colunas_booleanas(gx_df, list_of_columns:list):
    for column in list_of_columns:
        condicao_tipo = gx_df.expect_column_values_to_be_in_type_list(column, ['int', 'int64']).success
        condicao_valor_valido = gx_df.expect_column_values_to_be_in_set(column, [0, 1]).success
        if condicao_tipo and condicao_valor_valido:
            print(f"A coluna '{column}' é válida. (Tipo e Valor)")
        else:
            if not condicao_tipo:
                print(f"ERRO: A coluna '{column}' não está no tipo correto. Esperado: int.")
            if not condicao_valor_valido:
                print(f"ERRO: A coluna '{column}' tem valores inválidos.")                

def verificar_colunas_com_none(gx_df, list_of_columns:list):
    for column in list_of_columns:
        condicao_valores_nulos = gx_df.expect_column_values_to_not_be_null(column).success
        if condicao_valores_nulos:
            print(f"A coluna '{column}' é válida. (Não tem valores nulos)")
        else:
            print(f"ERRO: A coluna '{column}' contém valores nulos.")

def verificar_valores_min_max(gx_df, list_of_columns:list, min, max):
    for column in list_of_columns:
        condicao_valores = gx_df.expect_column_values_to_be_between(column, min_value=min, max_value=max).success
        if condicao_valores:
            print(f"A coluna '{column}' está entre os valores de {min} e {max}.")
        else:
            print(f"ERRO: A coluna '{column}' não está entre os valores de {min} e {max}.")

def verificar_colunas_id(gx_df, list_of_id_columns:list):

    correct_types = ['int', 'int64']
    for column in list_of_id_columns:
        condicao_tipo = gx_df.expect_column_values_to_be_in_type_list(column, correct_types).success
        condicao_valores_nulos = gx_df.expect_column_values_to_not_be_null(column).success
        valor_max_bigint = 9223372036854775807
        condicao_valores = gx_df.expect_column_values_to_be_between(column, min_value=0, max_value=valor_max_bigint).success

        if condicao_tipo and condicao_valores_nulos and condicao_valores:
            print(f"A coluna '{column}' é válida. (Tipo e Valor)")
        else:
            if not condicao_tipo:
                print(f"ERRO: A coluna '{column}' não está no tipo correto. Esperado: ",' ou '.join(correct_types))
            if not condicao_valores_nulos:
                print(f"ERRO: A coluna '{column}' possui valore(s) nulos.")
            if not condicao_valores:
                print(f"ERRO: A coluna '{column}' possui valore(s) acima ou abaixo do range permitido {[0,valor_max_bigint]}.")
                