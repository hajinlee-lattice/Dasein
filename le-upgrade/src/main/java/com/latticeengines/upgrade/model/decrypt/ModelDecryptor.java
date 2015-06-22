package com.latticeengines.upgrade.model.decrypt;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import sun.misc.BASE64Decoder;

public class ModelDecryptor {

    public static String decrypt(String encryptedContent) throws IOException, NoSuchAlgorithmException,
            NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException, IllegalBlockSizeException,
            BadPaddingException {
        Key key = new SecretKeySpec(key128Bit(), "AES");
        IvParameterSpec ivParameterSpec = new IvParameterSpec(key128Bit());
        Cipher c = Cipher.getInstance("AES/CBC/PKCS5Padding");
        c.init(Cipher.DECRYPT_MODE, key, ivParameterSpec);
        byte[] decordedValue = new BASE64Decoder().decodeBuffer(encryptedContent);
        byte[] decValue = c.doFinal(decordedValue);
        return new String(decValue);
    }

    private static byte[] key128Bit() throws UnsupportedEncodingException {
        byte[] key = new byte[16];
        byte[] startKey = "Bard4Everyone!".getBytes("UTF8");
        for (int i = 0; i < 16; i += startKey.length) {
            if (16 - i < startKey.length) {
                System.arraycopy(startKey, 0, key, i, 16 - i);
            } else
                System.arraycopy(startKey, 0, key, i, startKey.length);
        }
        return key;
    }
    
    public static void main(String[] args) throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException, IOException{
        System.out.println(decrypt(""));
    }
}
