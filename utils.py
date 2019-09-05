from Crypto.Cipher import DES
import base64

class Encryption(object):
    des = DES.new('omgr2012', DES.MODE_ECB)

    def encrypt(self, message):
        message += '.' * (8 - len(message) % 8)
        return base64.b64encode(self.des.encrypt(message))
    
    def decrypt(self, encoded_encrypted_message):
        encrypted_message = base64.b64decode(encoded_encrypted_message)
        return self.des.decrypt(encrypted_message).decode('utf-8').replace('.','')