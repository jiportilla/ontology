from base import BaseObject
from base import CryptoBase
from base import FileIO
import argparse


class CryptoAPI(BaseObject):

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            9-Oct-2019
            thanh@us.ibm.com
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._config = FileIO.file_to_yaml_by_relative_path(self.__config_path)


if __name__ == "__main__":

    # Parsing command line arguments
    parser = argparse.ArgumentParser(description='Encryption and Decryption API')
    parser.add_argument('--type', '-t', required=True, help='encrypt or decrypt', default=None)
    parser.add_argument('--source_text', '-s', required=True, help='encrypt or decrypt', default=None)

    args = parser.parse_args()

    print (args.type)
    if ( args.type == 'encrypt'):
        print(CryptoBase.encrypt_str(args.source_text))
    elif ( args.type == 'decrypt') :
        print(CryptoBase.decrypt_str(args.source_text))
    else:
        print("Type is not supported")
        #CryptoAPI.self.logger.error(f"Type is not supported: {args.type}")
