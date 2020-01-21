package com.latticeengines.saml.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.lang3.StringUtils;
import org.opensaml.Configuration;
import org.opensaml.saml2.metadata.EntityDescriptor;
import org.opensaml.xml.XMLObject;
import org.opensaml.xml.io.Marshaller;
import org.opensaml.xml.io.Unmarshaller;
import org.opensaml.xml.parse.ParserPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public final class SAMLUtils {

    protected SAMLUtils() {
        throw new UnsupportedOperationException();
    }

    private static final Logger log = LoggerFactory.getLogger(SAMLUtils.class);

    private static final String ALIAS = "/alias/";

    public static String getTenantIdFromLocalEntityId(String entityId) {
        log.info("entityId = " + entityId);
        if (StringUtils.isNotEmpty(entityId) && entityId.contains("/")) {
            entityId = entityId.endsWith("/") ? entityId.substring(0, entityId.lastIndexOf("/")) : entityId;
            if (entityId.contains("/")) {
                return entityId.substring(entityId.lastIndexOf("/") + 1);
            }
        }
        return null;
    }

    public static XMLObject deserialize(ParserPool parser, String string) {
        try (InputStream stream = new ByteArrayInputStream(string.getBytes())) {
            EntityDescriptor descriptor = (EntityDescriptor) deserialize(parser, stream);

            if (StringUtils.isEmpty(descriptor.getEntityID())) {
                throw new RuntimeException("Entity ID is not present in Idp metadata XML");
            }
            return descriptor;
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
