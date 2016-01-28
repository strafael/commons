# -*- coding: utf-8 -*-

# The MIT License (MIT)
#
# Copyright (c) 2015 Rafael Santos (rstogo@outlook.com)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""Check checksums of files.
Liberally based on: https://bitbucket.org/prologic/tools/src/tip/md5sum

Use `hashlib.algorithms_guaranteed` to view the names of the hash algorithms
guaranteed to be supported by this module on all platforms.

Use `hashlib.algorithms_available` to view the names of the hash algorithms
that are available in the running Python interpreter.
"""

import sys
import hashlib

CHUNKSIZE = 1024


def checksum(filename, algorithm='md5', binary=True):
    m = hashlib.new(algorithm)

    if binary:
        mode = 'rb'
    else:
        mode = 'r'

    with open(filename, mode) as f:
        for chunk in iter(lambda: f.read(CHUNKSIZE * m.block_size), b""):
            m.update(chunk)

    return m.hexdigest()


def main():
    if len(sys.argv) < 2:
        raise SystemExit(1)

    print(checksum(sys.argv[1]))


if __name__ == '__main__':
    main()
