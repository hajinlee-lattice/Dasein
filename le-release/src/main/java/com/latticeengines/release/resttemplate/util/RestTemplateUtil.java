package com.latticeengines.release.resttemplate.util;

import org.apache.commons.codec.binary.Base64;


public class RestTemplateUtil {

    public static String encodeToken(String creds){
        byte[] plainCredsBytes = creds.getBytes();
        byte[] base64CredsBytes = Base64.encodeBase64(plainCredsBytes);
        return new String(base64CredsBytes);
    }
}
