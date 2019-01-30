package com.latticeengines.common.exposed.util;

import java.util.Random;
import java.util.UUID;

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
        Assert.assertEquals(decrypted, strToEncrypt);
    }

    @Test(groups = "unit")
    public void testEncrytionAndDecryptionRandomString() throws Exception {
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < 100; i++){
            String strToEncrypt = UUID.randomUUID().toString().replace("-", "");
            strToEncrypt = strToEncrypt.substring(0, random.nextInt(strToEncrypt.length()));
            String encrypted = CipherUtils.encrypt(strToEncrypt);
            String decrypted = CipherUtils.decrypt(encrypted);
            Assert.assertEquals(strToEncrypt, decrypted);
        }
    }

    @Test(groups = "unit")
    public void testBackwardCompatibility() throws Exception {
        // NOTE these two are using legacy IV spec, using these to make sure we can
        // still decrypt old cipher texts
        Assert.assertEquals(CipherUtils.decrypt("hjl5F8+oM0X9tBVaI56E6Q=="), "Lattice123");
        Assert.assertEquals(CipherUtils.decrypt("KPpl2JWz+k79LWvYIKz6cA=="), "welcome");
        // encoded using legacy IV spec, make sure it still works
        Assert.assertEquals(CipherUtils.decryptBase58("7DnDQMq5qbUb4JzjkKroyM"), "hello");
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
