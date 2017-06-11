import base64
from Crypto import Random
from Crypto.Cipher import AES

__KEY = b'I03TMIIUftFUUI7bV0zFBw=='

def __pad(s):
    while len(s) % 16 > 0:
        s += ' '
    return s

def encrypt(raw):
    if raw[-1] == ' ':
        raise Exception('the message to be encrypted cannot end with space.')
    iv = Random.new().read(AES.block_size)
    cipher = AES.new(__KEY, AES.MODE_CBC, iv)
    return base64.urlsafe_b64encode(iv + cipher.encrypt(__pad(raw)))

def decrypt(encrypted):
    decoded = base64.urlsafe_b64decode(encrypted)
    iv = decoded[:16]
    msg = decoded[16:]
    cipher = AES.new(__KEY, AES.MODE_CBC, iv)
    raw = cipher.decrypt(msg)
    while raw[-1] == ' ':
        raw = raw[:-1]
    return raw

if __name__ == '__main__':
    enc = encrypt("welcome")
    print enc
    print '|' + decrypt(enc) + '|'