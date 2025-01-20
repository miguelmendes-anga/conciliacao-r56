import logging
import awswrangler as wr
from pandas.errors import ParserError
from datetime import datetime
import pandas as pd


logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def r56_reader(bucket, key):
    logger.info('Lendo arquivo')
    try:
        df = wr.s3.read_csv(f's3://{bucket}/{key}', encoding='iso8859-1', sep=';')
    except ParserError as e:
        if 'Error tokenizing data. C error: Expected 1 fields' in str(e):
            skip_rows = 4 if key.rsplit('.', 1)[-1].lower() in ['csv', 'rem'] else 6
            df = wr.s3.read_csv(f's3://{bucket}/{key}', encoding='iso8859-1', sep=';', skiprows=skip_rows)
    except Exception:
        raise
    
    return df


def determine_date_format(date_str):
    date_patterns = ["%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y %H:%M"]

    for pattern in date_patterns:
        try:
            datetime.strptime(date_str, pattern)
            return pattern
        except:
            pass
    else:
        raise Exception("Formato de data não reconhecido")


def clean_and_format_data(df, key):
    logger.info("Tratando dataframe")
    df = rename_cols(df)

    if '/BMP/' in key and 'OPERACOES DE ALIENACAO E CESSAO FIDUCIARIA' in df.columns:
        df = df.iloc[4:].reset_index(drop=True)

    origin_folder = key.split('/')[1].lower().replace(' ', '_')
    datetime_format = determine_date_format(df['dt_pedido'].iloc[0])
    date_format = determine_date_format(df['dt_prevista_repasse'].iloc[0])

    df['cpf_cnpj_sacado'] = df['cpf_cnpj_sacado'].astype('string').str.replace('.', '').str.replace('-', '').str.replace('/', '').str.zfill(11)
    df['dt_pedido'] = pd.to_datetime(df['dt_pedido'], format=datetime_format)
    df['dt_prevista_repasse'] = pd.to_datetime(df['dt_prevista_repasse'], format=date_format).dt.date
    df['dt_efetiva_pagamento'] = pd.to_datetime(df['dt_efetiva_pagamento'], format=date_format).dt.date
    df['numero_protocolo'] = df['numero_protocolo'].astype('string').str.replace('.0', '')
    df['identificador_solicitacao'] = df['identificador_solicitacao'].astype('string').str.replace('.0', '')
    df.loc[:, 'origem'] = origin_folder

    today = datetime.today()
    df.loc[:, 'year'] = today.strftime('%Y')
    df.loc[:, 'month'] = today.strftime('%m')
    df.loc[:, 'day'] = today.strftime('%d')

    if df['cpf_cnpj_sacado'].iloc[-1] == 'FIM DO RELATORIO':
        df = df.iloc[:-1]

    if 'vl_repassado' not in df.columns:
        df.loc[:, 'vl_repassado'] = 0.0

    if 'vl_cedido_atualizado' not in df.columns:
        df.loc[:, 'vl_cedido_atualizado'] = 0.0

    df = apply_dtypes(df)

    df = correct_status(df)

    cols_to_keep = ['cpf_cnpj_sacado', 'dt_pedido', 'identificador_solicitacao', 'status_periodo', 'dt_prevista_repasse', 'vl_cedido_original', 'vl_cedido_atualizado', 'numero_protocolo', 'dt_efetiva_pagamento', 'vl_repassado', 'origem', 'year', 'month', 'day']

    return df[cols_to_keep]


def correct_status(df):
    depara = {
        'QUITADA': 3,
        'CANCELADA': 2,
        'PAGO': 1,
        'NÃO ACATADO': 1,
        'EM PROCESSO DE PAGAMENTO': 1,
        'EM PROCESSO DE CANCELAMENTO': 1,
        'EM PROCESSAMENTO': 1,
        'GARANTIDA': 0
    }

    df['status_periodo'] = df['status_periodo'].apply(lambda x: f'{depara.get(x, 4)} - {x}')

    return df

def apply_dtypes(df):
    logger.info("Aplicando tipagem de colunas")

    dtypes = {
        'cpf_cnpj_sacado': 'string',
        'dt_pedido': 'datetime64[ns]',
        'identificador_solicitacao': 'string',
        'status_periodo': 'string',
        'vl_cedido_original': 'double',
        'vl_cedido_atualizado': 'double',
        'numero_protocolo': 'string',
        'vl_repassado': 'double',
        'origem': 'string',
        'year': 'string',
        'month': 'string',
        'day': 'string',
    }

    for col, dtype in dtypes.items():
        try:
            if dtype == 'double' and ',' in str(df[col].iloc[0]):
                df[col] = df[col].astype('string').str.replace('.', '').str.replace(',', '.').astype('double')
                continue

            df[col] = df[col].astype(dtype)
        except KeyError:
            continue
            
    return df


def rename_cols(df):
    logger.info("Renomeando colunas")

    df.columns = df.columns.str.upper()

    renamed_cols = {
        'CPF': 'cpf_cnpj_sacado',
        'DATA DO PEDIDO': 'dt_pedido',
        'IDENTIFICADOR DA SOLICITACAO': 'identificador_solicitacao',
        'TIPO DA OPERACAO': 'tipo_operacao',
        'CANAL DE SOLICITACAO': 'canal_solicitacao',
        'STATUS DO PERIODO': 'status_periodo',
        'DATA PREVISTA REPASSE': 'dt_prevista_repasse',
        'VALOR CEDIDO/ALIENADO ORIGINAL': 'vl_cedido_original',
        'VALOR CEDIDO/ALIENADO ATUALIZADO': 'vl_cedido_atualizado',
        'NUMERO DO PROTOCOLO': 'numero_protocolo',
        'STATUS DO PROTOCOLO': 'status_protocolo',
        'DATA EFETIVA DE PAGAMENTO': 'dt_efetiva_pagamento',
        'VALOR REPASSADO': 'vl_repassado',
        'IDENTIFICADOR': 'identificador_solicitacao'
    }

    df = df.rename(columns=renamed_cols)
    
    return df