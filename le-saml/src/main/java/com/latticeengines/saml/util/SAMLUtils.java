package com.latticeengines.saml.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.opensaml.Configuration;
import org.opensaml.xml.XMLObject;
import org.opensaml.xml.io.Marshaller;
import org.opensaml.xml.io.Unmarshaller;
import org.opensaml.xml.parse.ParserPool;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class SAMLUtils {

    public static final String ALIAS = "/alias/";
    public final static String LOCAL_ENTITY_ID_BASE = "app.lattice-engines.com/";

    public static String getEntityIdFromTenantId(String tenantId) {
        return LOCAL_ENTITY_ID_BASE + tenantId;
    }

    public static String getTenantIdFromLocalEntityId(String entityId) {
        if (entityId.startsWith(LOCAL_ENTITY_ID_BASE)) {
            return entityId.replace(LOCAL_ENTITY_ID_BASE, "");
        }
        return null;
    }

    public static XMLObject deserialize(ParserPool parser, String string) {
        try (InputStream stream = new ByteArrayInputStream(string.getBytes())) {
            return deserialize(parser, stream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static XMLObject deserialize(ParserPool parser, InputStream inputStream) {
        try {
            Document doc = parser.parse(inputStream);
            Unmarshaller unmarshaller = Configuration.getUnmarshallerFactory()
                    .getUnmarshaller(doc.getDocumentElement());
            return unmarshaller.unmarshall(doc.getDocumentElement());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String serialize(XMLObject xmlObject) {
        try {
            Marshaller marshaller = Configuration.getMarshallerFactory().getMarshaller(xmlObject);
            Element dom = marshaller.marshall(xmlObject);
            DOMSource source = new DOMSource(dom);
            StringWriter writer = new StringWriter();
            StreamResult result = new StreamResult(writer);
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.transform(source, result);
            return writer.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String getTenantFromAlias(String requestURI) {
        int filterIndex = getAliasIndex(requestURI);
        return requestURI.substring(filterIndex + ALIAS.length());
    }

    public static int getAliasIndex(String requestURI) {
        return requestURI.indexOf(ALIAS);
    }
}
