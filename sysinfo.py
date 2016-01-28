# -*- coding: utf-8 -*-

"""Obtém informações de sistema.

Ao usar o método prettyprint(), é retornado uma string com conteúdo como o
abaixo:

----------------------
Informações do Sistema
----------------------
    Sistema operacional : 7 6.1.7601 SP1 Multiprocessor Free
            Processador : Intel64 Family 6 Model 44 Stepping 2, GenuineIntel
Memória instalada (RAM) : 12.11 GB
     Memória disponível : 7.56 GB
        Tipo de sistema : AMD64
     Nome do computador : MI00272143
                 Python : 3.5.1 |Anaconda 2.4.1 (64-bit)
                Usuário : rtogo

"""

import sys
import os
import platform
import psutil
from collections import OrderedDict


def getsysteminfo():
    sysinfo = OrderedDict()

    sysinfo['    Sistema operacional '] = ' '.join(platform.win32_ver())
    sysinfo['            Processador '] = platform.processor()

    sysinfo['Memória instalada (RAM) '] = \
        '{:.2f} GB'.format(psutil.virtual_memory()[0] / 1024 / 1024 / 1014)

    sysinfo['     Memória disponível '] = \
        '{:.2f} GB'.format(psutil.virtual_memory()[1] / 1024 / 1024 / 1024)

    sysinfo['        Tipo de sistema '] = platform.machine()
    sysinfo['     Nome do computador '] = platform.node()
    sysinfo['                 Python '] = sys.version
    sysinfo['                Usuário '] = os.getlogin()

    return sysinfo


def prettyprint():
    import textwrap

    header = textwrap.dedent(
        """
        ----------------------
        Informações do Sistema
        ----------------------
        """)

    sysinfo = getsysteminfo()
    text = '\n'.join(['{}: {}'.
        format(k, v) for k, v in sysinfo.items()])
    prettytext = header + text + os.linesep
    return prettytext


def main():
    info = prettyprint()
    print(info)

if __name__ == '__main__':
    main()
