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

import base64
import json
import getpass
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
        >>> kc.list
        ['SAP', 'imap']
        
        >>> kc['SAP']
        {'username': 'John', 'password': 'user password'}

        >>> kc.set('SAP', language='en')
        >>> kc['SAP']
        {'username': 'John', 'password': 'user password', 'language': 'en'}

        >>> kc.save()
        >>> with open('passwords.txt', 'r') as fp:
        >>>     print(fp.read())
        gV2RTYWO31hb8bR/hD3KmhJHQ8Us36DC
        9p9u0jbViNd/bPWnB9vZjr0v/YpdnS8M
        JFxL97DUkZE2ZDEtj/WuxEFkfclv8bea
        6HsWHK8pqT/yBaNvQjHBvqMCewSlZ4sW
        fIbgnxAgNGdXp3RMZmE6ehmxx6SZMNVB
        CcUBH/6fGaSTHBiArqs=

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

    def list(self):
        """Returns all entry names currently stored in keychain"""
        return list(self._entries.keys())

    def save(self, password=None):
        """Stores the keychain on-disk"""
        if password:
            self._encryption_key = self._password_to_key(password)

        if self.dirty or password:
            with open(self._path, 'w') as fp:
                encrypted = self._encrypt()
                fp.write(self._split_lines(encrypted))

    def _load(self):
        """Load an existing keychain database"""
        with open(self._path, 'r') as fp:
            blob = ''.join(fp.read())
            return self._decrypt(blob)

    def _password_to_key(self, password):
        """Converts a string password to a 32 bytes length key.

        This needs to be done because keys used with AES encryption algorithm
        must be 16, 24 or 32 bytes long. So we hashes the password with the
        SHA-256 algorithm, witch returns a 32 bytes long key.

        """
        return SHA256.new(password.encode()).digest()

    def _split_lines(self, string, every=32):
        """Insert a new line every N characters"""
        return '\n'.join(string[i:i+every]
                         for i in range(0, len(string), every))

    def _encrypt(self):
        """Encrypt the keychain"""
        data = json.dumps(self._entries).encode()
        if not self._iv:
            self._iv = Random.get_random_bytes(AES.block_size)
        aes = AES.new(self._encryption_key, self._mode, self._iv)

        return base64.b64encode(self._iv + aes.encrypt(data)).decode()

    def _decrypt(self, blob):
        """Decrypt the keychain data"""
        blob = base64.b64decode(blob.encode())
        if not self._iv:
            self._iv = blob[:AES.block_size]
        aes = AES.new(self._encryption_key, self._mode, self._iv)
        
        data = aes.decrypt(blob[AES.block_size:]).decode()
        return json.loads(data)


if __name__ == '__main__' :
    kc = KeyChain('passwords.txt', password='123')
    print('Loaded keychain: {}'.format(kc.list))

    kc.set('SAP', username='John', password='John123')
    kc.set('imap', user='John', password='')
    print(kc.list)
    print(kc['SAP'])

    kc.set('SAP', language='en')
    print(kc['SAP'])

    kc.save()
