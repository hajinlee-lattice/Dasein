package com.latticeengines.security.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import com.onelogin.saml2.util.Util;

public class SAMLParsingUtils {

    private static final Logger log = LoggerFactory.getLogger(SAMLParsingUtils.class);

    public static Document parseSAMLRequest(String samlRequestParameter) {
        String samlRequestStr = Util.base64decodedInflated(samlRequestParameter);
        log.info("SAMLRequest: " + samlRequestStr);
        return Util.loadXML(samlRequestStr);
    }

    public static Document parseSAMLResponse(String samlResponseParameter) {
        String samlResponseStr = Util.base64decodedInflated(samlResponseParameter);
        log.info("SAMLResponse: " + samlResponseStr);
        return Util.loadXML(samlResponseStr);
    }

    public static String extractIssuer(Document document) {
        if (document.getElementsByTagName("saml:Issuer").getLength() > 0) {
            return document.getElementsByTagName("saml:Issuer").item(0).getTextContent();
        } else {
            return null;
        }
    }

    public static String extractNameID(Document document) {
        if (document.getElementsByTagName("saml:NameID").getLength() > 0) {
            return document.getElementsByTagName("saml:NameID").item(0).getTextContent();
        } else {
            return null;
        }
    }

}
