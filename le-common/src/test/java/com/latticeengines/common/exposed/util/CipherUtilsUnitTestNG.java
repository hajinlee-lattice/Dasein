package com.latticeengines.common.exposed.util;

import org.apache.commons.codec.binary.Base64;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CipherUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testEncrytionAndDecryption() throws Exception {
        String strToEncrypt = "welcome";
        String encrypted = CipherUtils.encrypt(strToEncrypt);
        String decrypted = CipherUtils.decrypt(encrypted);
        System.out.println("Encrypted: " + encrypted + "\n" + "Decrypted: " + decrypted);
        Assert.assertEquals(strToEncrypt, decrypted);
    }

    @Test(groups = "unit")
    public void testEncrytionAndDecryptionBase58() throws Exception {
        String strToEncrypt = "Customer1.Customer1.Production|43567";
        String encrypted = CipherUtils.encryptBase58(strToEncrypt);
        String decrypted = CipherUtils.decryptBase58(encrypted);
        System.out.println("Encrypted: " + encrypted + "\n" + "Decrypted: " + decrypted);
        Assert.assertEquals(strToEncrypt, decrypted);
    }

    @Test(groups = "unit")
    public void testKeyGeneration() throws Exception {
        String key1 = CipherUtils.generateKey();
        String key2 = Base64.encodeBase64String(CipherUtils.strToKey(key1).getEncoded());
        System.out.println(key1 + "\n" + key2);
        Assert.assertEquals(key1, key2);
    }
}
