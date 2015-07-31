package com.latticeengines.release.resttemplate.util;

import org.apache.commons.codec.binary.Base64;


public class RestTemplateUtil {

    public static String encodeToken(String creds){
        String plainCreds = creds;
        byte[] plainCredsBytes = plainCreds.getBytes();
        byte[] base64CredsBytes = Base64.encodeBase64(plainCredsBytes);
        String base64Creds = new String(base64CredsBytes);
        return base64Creds;
    }
}
