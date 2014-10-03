package com.latticeengines.camille;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentMetadata;

public class DocumentSerializer {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    // To be stored in an individual zookeeper znode
    private static class Node {
        public String data;
        public DocumentMetadata metadata;
    }

    public static Document toDocument(byte[] data) throws DocumentSerializationException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

        Node node;
        try {
            String string = new String(data, "UTF-8");
            node = mapper.readValue(string, Node.class);
        } catch (Exception e) {
            String msg = "Error deserializing from data " + data;
            log.error(msg, e);
            throw new DocumentSerializationException(msg, e);
        }

        return new Document(node.data, node.metadata);
    }

    public static byte[] toByteArray(Document document) throws DocumentSerializationException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

        Node node = new Node();
        node.data = document.getData();
        node.metadata = document.getMetadata();

        try {
            String string = mapper.writeValueAsString(node);
            return string.getBytes("UTF-8");
        } catch (Exception e) {
            String msg = "Error serializing document " + document;
            log.error(msg, e);
            throw new DocumentSerializationException(msg, e);
        }
    }
}
