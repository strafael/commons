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

"""KeyChain is a simple and convenient sensitive data manager.

You can put any number of info associated with an entry in the keychain,
database, which is locked with one master key.
The database is encrypted using AES encryption algorithm.

"""

import json
import getpass
import binascii
from Crypto import Random
from Crypto.Cipher import AES
from Crypto.Hash import SHA256


class KeyChain(object):
    """Simple on-disk KeyChain storage.

    Args:
        path (str): Path of the file to store the keychain.
        password (Optional[str]): The master key to encrypt the database.
            If it's not provided, the user will be prompted for a password.

    Examples:
        >>> kc = KeyChain('passwords.txt', password='master password')
        >>> kc.set('SAP', username='John', password='John123')
        >>> kc.set('imap', user='John', password='')
        >>> kc['SAP']
        {'username': 'John', 'password': 'user password'}

        >>> kc.set('SAP', language='en')
        >>> kc['SAP']
        {'username': 'John', 'password': 'user password', 'language': 'en'}

        >>> kc.save()
        >>> with open('passwords.txt', 'r') as fp:
        >>>     print(fp.read())
        3b9b 038e d5a0 d129 03ca 5eb7 d024 ccc6
        b37e 28c3 2b7e 5ebf ebb3 82a9 b46a 6749
        a41e d1a1 ddb7 b7d0 a398 5d91 f80e 6ec6
        4b87 f0ed 7f17 31d5 b39b ca85 ae3b dc12
        472e 8372 0116 ddca 35df ee0f 8d88 c96e

    """
    def __init__(self, path, password=None):
        self._path = path
        if not password:
            password = getpass.getpass()

        self._encryption_key = self._password_to_key(password)
        self._entries = dict()
        self._mode = AES.MODE_CFB
        self._iv = None
        self.dirty = False  # It's set to true when self._entries is modified
        
        try:
            # Try to read an existing keychain file
            self._entries = self._load()
        except IOError:
            # File doesn't exist yet
            self.save()
        except (ValueError, UnicodeDecodeError):
            # Wrong password
            raise ValueError("Incorrect password")

    def __getitem__(self, name):
        """Returns all data associated with an entry"""
        return self._entries.get(name)

    def __delitem__(self, name):
        """Remove an entry from the keychain"""
        del self._entries[name]
        self.dirty = True

    def set(self, name, **data):
        """Insert/Update a keychain entry"""
        try:
            self._entries[name].update(data)
        except KeyError:
            self._entries[name] = data
        self.dirty = True

    @property
    def list(self):
        """Returns all entry names currently stored in keychain"""
        return list(self._entries.keys())

    def save(self, password=None):
        """Stores the keychain on-disk"""
        if password:
            self._encryption_key = self._password_to_key(password)

        if self.dirty or password:
            encrypted = self._encrypt()
            self._dump(encrypted)

    def _load(self):
        """Load an existing keychain database"""
        import re

        e = re.compile(r'\s+') # Matches all whitespace characters
        with open(self._path, 'r') as fp:
            blob = e.sub('', fp.read())
            return self._decrypt(blob)

    def _encrypt(self):
        """Encrypt the keychain"""
        data = json.dumps(self._entries).encode()
        if not self._iv:
            self._iv = Random.get_random_bytes(AES.block_size)
        aes = AES.new(self._encryption_key, self._mode, self._iv)

        return binascii.hexlify(self._iv + aes.encrypt(data)).decode()

    def _decrypt(self, blob):
        """Decrypt the keychain data"""
        blob = binascii.unhexlify(blob.encode())
        if not self._iv:
            self._iv = blob[:AES.block_size]
        aes = AES.new(self._encryption_key, self._mode, self._iv)
        
        data = aes.decrypt(blob[AES.block_size:]).decode()
        return json.loads(data)

    def _password_to_key(self, password):
        """Converts a string password to a 32 bytes length key.

        This needs to be done because keys used with AES encryption algorithm
        must be 16, 24 or 32 bytes long. So we hashes the password with the
        SHA-256 algorithm, witch returns a 32 bytes long key.

        """
        return SHA256.new(password.encode()).digest()

    def _split_lines(self, string, space_every=4, newline_every=39):
        """Align data inserting space and new line every N characters"""
        string = ' '.join(string[i:i+space_every]
                          for i in range(0, len(string), space_every))
        string = '\n'.join(string[i:i+newline_every]
                           for i in range(0, len(string), newline_every))
        return string

    def _dump(self, string, block_width=4):
        """Dump a string into a file grouped in blocks.
        
        Args:
            string (str): String to be dumped.
            block_width (Optional[int]) Block size.

        """
        with open(self._path, 'w') as fp:
            for line in self._get_lines(string):
                line = ' '.join(line[i:i+block_width]
                                for i in range(0, len(line), block_width))
                fp.write(line + '\n')

    def _get_lines(self, string, width=32):
        """Split a string in lines.
        
        Args:
            string (str): String to be splitted.
            width (Option[int]): Line length.

        Returns:
            Generator of lines with `width` length.

        """
        for i in range(0, len(string), width):
            yield string[i:i+width]


if __name__ == '__main__' :
    kc = KeyChain('.keychain', password='123')
    print('Loaded keychain: {}'.format(kc.list))

    kc.set('SAP', username='John', password='John123')
    kc.set('imap', user='John', password='')
    print(kc.list)
    print(kc['SAP'])

    kc.set('SAP', language='en')
    print(kc['SAP'])

    kc.save()
