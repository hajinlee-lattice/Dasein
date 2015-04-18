package com.latticeengines.domain.exposed.admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SerializableDocumentDirectory {

    private String rootPath;
    private Collection<Node> nodes;

    private DocumentDirectory documentDirectory;

    public SerializableDocumentDirectory() {
    }

    public SerializableDocumentDirectory(DocumentDirectory documentDirectory) {
        this.setDocumentDirectory(documentDirectory);
        this.rootPath = documentDirectory.getRootPath().toString();
        Collection<Node> nodes = new ArrayList<>();
        for (DocumentDirectory.Node node : documentDirectory.getChildren()) {
            nodes.add(new Node(node));
        }
        if (!nodes.isEmpty()) this.nodes = nodes;
    }

    public JsonNode applyMetadata (DocumentDirectory metadataDirectory) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode jNode = mapper.valueToTree(this);
        // apply metadata to nodes
        Collection<Node> docNodes = this.getNodes();
        if (docNodes != null) {
            ArrayNode aNode = mapper.createArrayNode();
            for (Node node : docNodes) {
                DocumentDirectory.Node metaNode = metadataDirectory.get("/" + node.getNode());
                JsonNode childNode = node.applyMetadata(metaNode);
                aNode.add(childNode);
            }
            jNode.put("Nodes", aNode);
        }
        return jNode;
    }

    public static DocumentDirectory deserialize(String jsonStr) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return deserialize(mapper.readTree(jsonStr));
    }

    public static DocumentDirectory deserialize(JsonNode jsonDir) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode textifiedDir = textifyJsonDir(jsonDir);
        SerializableDocumentDirectory serializedDir = mapper.readValue(textifiedDir.toString(), SerializableDocumentDirectory.class);
        return deserialize(serializedDir);
    }

    private static JsonNode textifyJsonDir(JsonNode jDir) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode oNode = mapper.createObjectNode();
        oNode.put("RootPath", jDir.get("RootPath").asText());
        if (jDir.has("Nodes") && jDir.get("Nodes").size() > 0) {
            ArrayNode aNode = mapper.createArrayNode();
            for (JsonNode node : jDir.get("Nodes")) {
                aNode.add(textifyJsonNode(node));
            }
            oNode.put("Nodes", aNode);
        }
        return oNode;
    }

    private static JsonNode textifyJsonNode(JsonNode jNode) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode oNode = mapper.createObjectNode();
        oNode.put("Node", jNode.get("Node").asText());
        if (jNode.has("Data")) {
            oNode.put("Data", jNode.get("Data").asText());
        }
        if (jNode.has("Version")) {
            oNode.put("Version", jNode.get("Version").asInt());
        }
        if (jNode.has("Children") && jNode.get("Children").size() > 0) {
            ArrayNode aNode = mapper.createArrayNode();
            for (JsonNode node : jNode.get("Children")) {
                aNode.add(textifyJsonNode(node));
            }
            oNode.put("Children", aNode);
        }
        return oNode;
    }

    public static DocumentDirectory deserialize(SerializableDocumentDirectory serializedDir) {
        DocumentDirectory dir = new DocumentDirectory();
        deserializeNodes(dir, serializedDir.getNodes(), "");
        dir.makePathsAbsolute(new Path(serializedDir.getRootPath()));
        return dir;
    }

    private static void deserializeNodes(DocumentDirectory dir, Collection<Node> nodes, String rootPath) {
        for (Node node : nodes) {
            Document doc = new Document("");
            if (node.getData() != null) {
                doc = new Document(String.valueOf(node.getData()));
            }
            doc.setVersion(node.getVersion());
            Path path = new Path(rootPath + "/" + node.getNode());
            dir.add(path, doc);
            if (node.getChildren() != null && !node.getChildren().isEmpty()) {
                deserializeNodes(dir, node.getChildren(), path.toString());
            }
        }
    }

    @JsonIgnore
    public DocumentDirectory getDocumentDirectory() {
        return documentDirectory;
    }

    @JsonIgnore
    public void setDocumentDirectory(DocumentDirectory documentDirectory) {
        this.documentDirectory = documentDirectory;
    }

    @JsonProperty("RootPath")
    public String getRootPath() { return rootPath; }

    @JsonProperty("RootPath")
    public void setRootPath(String rootPath) { this.rootPath = rootPath; }

    @JsonProperty("Nodes")
    public Collection<Node> getNodes() { return nodes; }

    @JsonProperty("Nodes")
    public void setNodes(Collection<Node> nodes) { this.nodes = nodes; }

    @Override
    public String toString() {
        return this.toJson().toString();
    }

    public JsonNode toJson() {
        try {
            return this.applyMetadata(new DocumentDirectory(new Path(this.rootPath)));
        } catch (IOException e) {
            return null;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Node {

        private String node;
        private String data;
        private int version = -1;
        private Collection<Node> children;

        public Node(){}

        public Node(DocumentDirectory.Node documentNode) {
            this.node = documentNode.getPath().getSuffix();
            this.data = documentNode.getDocument().getData();
            this.version = documentNode.getDocument().getVersion();

            Collection<Node> children = new ArrayList<>();
            for (DocumentDirectory.Node child : documentNode.getChildren()) {
                children.add(new Node(child));
            }
            if (!children.isEmpty()) this.children = children;
        }

        @JsonProperty("Node")
        public String getNode() { return node; }

        @JsonProperty("Node")
        public void setNode(String node) { this.node = node; }

        @JsonProperty("Data")
        public String getData() { return data; }

        @JsonProperty("Data")
        public void setData(String data) { this.data = data; }

        @JsonProperty("Version")
        public int getVersion() { return version; }

        @JsonProperty("Version")
        public void setVersion(int version) { this.version = version; }

        @JsonProperty("Children")
        public Collection<Node> getChildren() { return children; }

        @JsonProperty("Children")
        public void setChildren(Collection<Node> children) { this.children = children; }

        public JsonNode applyMetadata (DocumentDirectory.Node metaNode) throws IOException {
            if (this.getData() == null) return null;

            ObjectMapper mapper = new ObjectMapper();
            ObjectNode jNode = mapper.valueToTree(this);

            if (this.getData().equals("")) {
                jNode.remove("Data");
                return jNode;
            }

            Metadata metadata = new Metadata();
            if (metaNode != null) {
                try {
                    mapper.readValue(metaNode.getDocument().getData(), Metadata.class);
                } catch (JsonMappingException e) {
                    //ignore
                }
            }
            String type = metadata.interpretType(this.getData());
            metadata.convertDataToType(jNode, type);

            // apply metadata to children
            Collection<Node> children = this.getChildren();
            if (children != null) {
                ArrayNode aNode = mapper.createArrayNode();
                for (Node node : children) {
                    DocumentDirectory.Node childMetaNode = null;
                    if (metaNode != null) {
                        childMetaNode = metaNode.getChild(node.getNode());
                    }
                    JsonNode childNode = node.applyMetadata(childMetaNode);
                    aNode.add(childNode);
                }
                jNode.put("Children", aNode);
            }

            return jNode;
        }

    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Metadata {
        private String type;
        private Collection<String> options;

        public Metadata(){}

        @JsonProperty("Type")
        public String getType() { return type; }

        @JsonProperty("Type")
        public void setType(String type) { this.type = type; }

        @JsonProperty("Options")
        public Collection<String> getOptions() { return options; }

        @JsonProperty("Options")
        public void setOptions(Collection<String> options) { this.options = options; }

        public void convertDataToType(ObjectNode jNode, String type) {
            if (type.toLowerCase().equals("string")) { return; }
            JsonNode data = jNode.get("Data");
            if (type.toLowerCase().equals("number")) {
                jNode.put("Data", data.asDouble());
            } else if (type.toLowerCase().equals("boolean")) {
                jNode.put("Data", data.asBoolean());
            }
        }

        public String interpretType(String data) {
            String type = this.getType();
            if (type == null) {
                if (isNumber(data)) {
                    type = "number";
                } else if (isBoolean(data)) {
                    type = "boolean";
                } else {
                    type = "string";
                }
            }
            return type;
        }

        private static boolean isNumber(String str)
        {
            try
            {
                Double.parseDouble(str);
            }
            catch(NumberFormatException nfe)
            {
                return false;
            }
            return true;
        }

        private static boolean isBoolean(String str)
        {
            return str.toLowerCase().equals("true") || str.toLowerCase().equals("false");
        }
    }
}
