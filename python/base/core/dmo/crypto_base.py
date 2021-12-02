#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

from cryptography.fernet import Fernet

enc = "utf-8"


class CryptoBase(object):
    def __init__(self):
        """
        Created:
            5-Jun-2019
            craig.trim@ibm.com
            *   https://nitratine.net/blog/post/encryption-and-decryption-in-python/
        """

    @staticmethod
    def _key():
        path = os.path.join(os.environ["CODE_BASE"], "resources/config/ecrpt/key.key")

        file = open(path, 'rb')
        key = file.read()  # The key will be type bytes
        file.close()

        return key

    @staticmethod
    def encrypt_str(some_input: str) -> str:
        return CryptoBase.encrypt(some_input.encode(enc))

    @staticmethod
    def encrypt(message: bytes) -> str:
        f = Fernet(CryptoBase._key())
        return f.encrypt(message)

    @staticmethod
    def decrypt_str(some_input: str) -> str:
        return CryptoBase.decrypt(some_input.encode(enc))

    @staticmethod
    def decrypt(message: bytes) -> str:
        f = Fernet(CryptoBase._key())
        return f.decrypt(message).decode(enc)


def main(param1, param2):
    def _action():
        if param1 == "encrypt":
            return CryptoBase.encrypt_str(param2)
        elif param1 == "decrypt":
            return CryptoBase.decrypt_str(param2)
        else:
            raise NotImplementedError("\n".join([
                "Unknown Param: {}".format(param1)]))

    print(_action())


if __name__ == "__main__":
    import plac

    plac.call(main)
