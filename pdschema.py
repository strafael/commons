# -*- coding: latin-1 -*-

"""Módulo contém funções para mapear e validar schemas em DataFrames.
"""

from collections import defaultdict
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def uniquify(values):
    """Adiciona um sufixo aos valores duplicados de uma lista.

    Example:
        >>> uniquify(['A', 'B', 'A', 'A'])
        ['A', 'B', 'A_2', 'A_3']

    Args:
        values (list): Lista de valores a serem deduplicados.

    Returns:
        list: Lista com valores deduplicados.

    """
    newvalues = list()
    seen = defaultdict(int)

    for value in values:
        seen[value] += 1

        if seen[value] > 1:
            newvalue = '{}_{}'.format(value, seen[value])
            logger.debug('{!r} renomeado para {!r}'.format(value, newvalue))
            newvalues.append(newvalue)
        else:
            newvalues.append(value)

    return newvalues


def mapcolumns(df, columnmap, remove=True):
    """Renomeia as colunas de um DataFrame.
    
    Args:
        df (pandas.DataFrame): DataFrame a ser modificado.
        columnmap (dict): Dicionário no formato {oldname: newname} com mapa de
            colunas a serem renomeadas. Ex:
                columnmap = {
                    'Nota'                  : 'nota',
                    'Tp.'                   : 'tipo_de_nota',
                    'Status sistema'        : 'status_do_sistema',
                }
        remove (Optional[bool]): Se True, remove colunas não encontradas
            em 'colummap'. Defaults to True.

    """
    def check():
        """Verifica se as colunas do DataFrame estão de acordo com o
        resultado esperado.

        """
        desired_columns = set(columnmap.values())

        if set(df.columns) != desired_columns \
            or len(df.columns) != len(desired_columns):
            raise(ValueError('Schema incosistente!\n'
                             'Recebido:\n{}\n'
                             'Esperado:\n{}'
                             .format(set(df.columns), desired_columns)))

    # Remove whitespaces
    df.rename(columns=lambda x: x.strip(), inplace=True)

    # Renomeia colunas duplicadas
    df.columns = uniquify(df.columns)

    # Renomeia colunas
    df.rename(columns=columnmap, inplace=True)

    # Remove colunas que não estão no mapa
    if remove:
        for c in df.columns:
            if c not in columnmap.values():
                logger.debug('Coluna {!r} não especificada na schema foi '
                             'removida'.format(c))
                df.drop(c, axis=1, inplace=True)

    # Verifica se ficou igual ao resultado esperado
    return check()


def strip(df):
    """Strip whiespaces e converte campos em branco para np.NaN.
    """
    for col in df.select_dtypes(include=['object']).columns:
        # Remove whitespaces dos campos
        df[col] = df[col].map(lambda x: x.strip() if pd.notnull(x) else x)

        # Alguns campos podem ficar = '' depois de remover os whitespaces,
        # então vamos converter para np.nan
        df[col] = df[col].map(lambda x: np.NaN if x == '' else x)

    return df


def ensure_primarykey(df, primarykey, drop=True):
    """Garante que um conjunto de colunas não se repete.

    Args:
        df (pd.DataFrame): DataFrame a ser verificado.
        drop (bool): Se True, remove as linhas duplicadas.
            Se False, raises error. Defaults to True

    """
    # Exclui linhas com primary key duplicada
    n0 = len(df)
    df.drop_duplicates(subset=primarykey, inplace=True)
    n1 = len(df)

    if drop:
        logger.debug('{} linhas duplicadas removidas, {} linhas restantes'
                     .format(n0-n1, n1))
    else:
        if n1 != n0:
            raise ValueError('{} valores duplicados para colunas {}'
                             .format(n0-n1, primarykey))

    # Exclui linhas com primary key em branco
    n0 = len(df)
    df.dropna(subset=primarykey, axis=0, how='any', inplace=True)
    n1 = len(df)

    logger.debug('{:d} linhas com primary key em branco excluídas, '
                 '{:d} linhas restantes'.format(n0-n1, n1))

