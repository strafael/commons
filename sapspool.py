# -*- coding: latin-1 -*-

"""Este módulo contém funções para converter spools do SAP em arquivos csv.
"""

import os.path
import re
import tempfile
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def clean_fixed_sap_spool(input_filepath, separator='\|', replacewith=' ',
                          spoolheader=66, encoding='latin-1'):
    """Corrige estrutura de colunas fixas spools do SAP.
    
    A correção é feita reescrevendo o arquivo fazendo com que separadores
    usados em posições diferentes das posições dos separadores encontrados na
    linha de cabeçalho sejam substituídos por caracteres em branco.
    
    A expressão regular usada é: r'{separator}+'
    Ex: re.sub(r'\t+', '\t')

    Args:
        input_filepath (str): Nome no arquivo de entrada.
        output_filepath (str): Nome do arquivo de saída.
        separator (Optional[str]): Separador de colunas usado no
            arquivo importado. Defaults to '\|'.
        replacewith (Optional[str]): Caractere para substituir os separadores
            encontrados em posições diferentes dos seperadores do cabeçalho.
            Defaults to ' '.
        header (Optional[int]): Números de linhas a serem ignoradas antes de
            ler o arquivo. Defaults to 66.
        encoding (Optional[str]): Encoding do arquivo. Defaults to 'latin-1'.

    Returns:
        Caminho do arquivo tratado.

    """
    regexp = r'{!s}'.format(separator)
    temp_filename = None

    with open(input_filepath, 'rt', encoding=encoding) as fin:
        with tempfile.NamedTemporaryFile(mode='wt', encoding=encoding,
                                         delete=False) as temp_file:
            temp_filename = temp_file.name

            for i in range(spoolheader):
                line = fin.readline()
            i += 1

            spoolheader = fin.readline()
            temp_file.write(spoolheader)

            headerpositions = [m.start() for m in re.finditer(regexp, spoolheader)]
            totalheaderpositions = len(headerpositions)

            logger.debug('Linha {} usada como cabeçalho; {} colunas encontradas'
                         .format(i+1, totalheaderpositions+1))

            for line in fin:
                i += 1
                positions = [m.start() for m in re.finditer(regexp, line)]

                if line == spoolheader or len(positions) < totalheaderpositions:
                    continue

                if len(positions) > totalheaderpositions:
                    line = list(line)
                    for p in positions:
                        if p not in headerpositions:
                            logger.debug('Substituindo separador em excesso na '
                                         'linha {}, posição {}'
                                         .format(i+1, p+1))
                            line[p] = replacewith
                    line = ''.join(line)

                temp_file.write(line)

    return temp_filename


def clean_cm07_spool(input_filepath, separator='\t', encoding='latin-1'):
    """Reescreve o arquivo incluindo as informações do centro de trabalho em
    cada linha.
    
    A expressões regulares usadas são:
        Cabeçalhos: r'^Centro trab\.\s+(\w+)\s+([\S\s]+)Cent\..+(.{4})(?=$)$'
        Dados     : r'^\s+\|'

    Args:
        input_filepath (str): Nome do arquivo.

    Returns:
        Caminho do arquivo tratado.

    """
    header_regexp = re.compile(
        r'^Centro trab\.\s+(\w+)\s+([\S\s]+)Cent\..+(.{4})(?=$)$')
    data_regexp = re.compile(r'^\s+\|')

    temp_filename = None

    with open(input_filepath, 'rt', encoding=encoding) as fin:
        with tempfile.NamedTemporaryFile(mode='wt', encoding=encoding,
                                         delete=False) as temp_file:
            temp_filename = temp_file.name

            # Escreve linha de cabeçalho
            temp_file.write('Centro trab|Descricao|Centro|Dia|Necessidade|'
                            'Capacid.útil|Carga|Capac.livre|Unid.\n')

            for line in fin:
                # Pesquisa dados de cabeçalho
                matches = re.findall(header_regexp, line)
                if matches:
                    extradata = '|'.join(matches[0])

                # Pesquisa capacidade
                if re.search(data_regexp, line):
                    cleaned_line = extradata + line.strip()
                    temp_file.write(cleaned_line)
                    temp_file.write('\n')

    return temp_filename


def read_sapspool(path, sep='|', skiprows=0, dtype=str, encoding='latin-1',
                  spool='clean_fixed_sap_spool'):
    """Função de conveniência que limpa um sap spool e retorna um DataFrame.
    """

    clean_func = globals()[spool]
    temp_file = clean_func(path)

    df = pd.read_csv(temp_file, sep=sep, skiprows=skiprows,
                     dtype=dtype, encoding=encoding, index_col=False)

    os.remove(temp_file)
    return df
