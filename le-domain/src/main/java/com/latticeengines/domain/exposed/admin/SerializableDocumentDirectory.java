package com.latticeengines.domain.exposed.admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.microsoft.windowsazure.storage.core.PathUtility;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SerializableDocumentDirectory {

    private String rootPath;
    private Collection<Node> nodes;

    private DocumentDirectory documentDirectory;

    public SerializableDocumentDirectory() {
    }

    public SerializableDocumentDirectory(DocumentDirectory documentDirectory) {
        constructByDocumentDirectory(documentDirectory);
    }

    public SerializableDocumentDirectory(Map<String, String> properties) {
        DocumentDirectory docDir = new DocumentDirectory(new Path("/"));
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            try {
                Path nodePath = new Path(entry.getKey());
                docDir.add(nodePath, new Document(entry.getValue()));
            } catch (IllegalArgumentException e) {
                //ignore
            }
        }
        constructByDocumentDirectory(docDir);
    }

    public SerializableDocumentDirectory(String configJson) {
        this(configJson, null);
    }

    public SerializableDocumentDirectory(String configJson, String metadataJson) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode configNode, metadataNodes;

        try {
            configNode = mapper.readTree(configJson);
            if (metadataJson != null) {
                metadataNodes = mapper.readTree(metadataJson).get("Nodes");
            } else {
                metadataNodes = null;
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid format of input json.", e);
        }

        Collection<Node> nodes;
        try {
            nodes = Metadata.applyMetadataOnJsonArrays(configNode.get("Nodes"), metadataNodes);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot apply the schema to the configuration.", e);
        }

        this.rootPath = "/";
        if (nodes != null && !nodes.isEmpty()) this.nodes = nodes;
        this.documentDirectory = SerializableDocumentDirectory.deserialize(this);
    }

    private void constructByDocumentDirectory(DocumentDirectory documentDirectory) {
        this.setDocumentDirectory(documentDirectory);
        this.rootPath = documentDirectory.getRootPath().toString();
        Collection<Node> nodes = new ArrayList<>();
        for (DocumentDirectory.Node node : documentDirectory.getChildren()) {
            nodes.add(new Node(node));
        }
        if (!nodes.isEmpty()) this.nodes = nodes;
    }

    public Map<String, String> flatten() {
        Map<String, String> result = new HashMap<>();

        if (this.documentDirectory == null)
            this.documentDirectory = SerializableDocumentDirectory.deserialize(this);

        this.documentDirectory.makePathsLocal();

        Iterator<DocumentDirectory.Node> iter = this.documentDirectory.breadthFirstIterator();
        while (iter.hasNext()) {
            DocumentDirectory.Node node = iter.next();
            if (node.getDocument() != null && !node.getDocument().getData().equals("")) {
                result.put(node.getPath().toString(), node.getDocument().getData());
            }
        }

        return result;
    }

    public void applyMetadata (DocumentDirectory metadataDirectory) {
        if (metadataDirectory != null && this.getNodes() != null) {
            // apply metadata to nodes
            for (Node node : this.getNodes()) {
                DocumentDirectory.Node metaNode = metadataDirectory.get("/" + node.getNode());
                node.applyMetadata(metaNode);
            }
        }
    }

    @JsonIgnore
    public DocumentDirectory getMetadataAsDirectory() {
        Path rootPath = new Path("/dummyroot");
        DocumentDirectory dir =  new DocumentDirectory(rootPath);
        if (this.getNodes() != null) {
            for (Node node : this.getNodes()) {
                node.writeMetadataToDir(dir, "");
            }
        }
        dir.makePathsLocal();
        return dir;
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

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Node {

        private String node;
        private String data;
        private Metadata metadata;
        private int version = -1;
        private Collection<Node> children;

        public Node(){}

        public Node(DocumentDirectory.Node documentNode) {
            this.node = documentNode.getPath().getSuffix();
            this.data = documentNode.getDocument().getData();
            if (this.data.equals("")) { this.data = null; }
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

        @JsonProperty("Metadata")
        public Metadata getMetadata() { return metadata; }

        @JsonProperty("Metadata")
        public void setMetadata(Metadata metadata) { this.metadata = metadata; }

        @JsonProperty("Version")
        public int getVersion() { return version; }

        @JsonProperty("Version")
        public void setVersion(int version) { this.version = version; }

        @JsonProperty("Children")
        public Collection<Node> getChildren() { return children; }

        @JsonProperty("Children")
        public void setChildren(Collection<Node> children) { this.children = children; }

        public void applyMetadata (DocumentDirectory.Node metaNode) {
            if (this.getData() == null || metaNode == null) return;

            Metadata metadataProvided;
            ObjectMapper mapper = new ObjectMapper();

            try {
                metadataProvided = mapper.readValue(metaNode.getDocument().getData(), Metadata.class);
            } catch (NullPointerException|IOException e) {
                return;
            }

            this.applySingleMetadata(metadataProvided);

            // apply metadata to children
            if (this.getChildren() != null) {
                for (Node child : this.getChildren()) {
                    DocumentDirectory.Node childMetaNode = metaNode.getChild(child.getNode());
                    child.applyMetadata(childMetaNode);
                }
            }
        }

        public void applySingleMetadata (Metadata metadata) {
            if (this.getData() == null) return;

            if (metadata != null && metadata.getType() != null && !metadata.getType().equals("")) {
                if (metadata.validateData(this.getData())) {
                    this.metadata = metadata;
                } else {
                    throw new IllegalArgumentException(
                            String.format("data \"%s\" does not match the specified schema %s",
                                    this.getData(), metadata.toString()));
                }
            } else {
                String type = interpretDataType();
                if (!type.equals("string")) {
                    Metadata interpretedMetadata = new Metadata();
                    interpretedMetadata.setType(type);
                    this.setMetadata(interpretedMetadata);
                }
            }
        }

        public void writeMetadataToDir(DocumentDirectory dir, String parentPath){
            String nodePath = parentPath + "/" + this.node;

            if (this.metadata != null && this.metadata.getType() != null
                    && !(this.metadata.getType().equals("") || this.metadata.getType().equals("string"))) {
                dir.add(nodePath, this.metadata.toString());
            }

            if (this.getChildren() != null) {
                for (Node child : this.getChildren()) {
                    child.writeMetadataToDir(dir, nodePath);
                }
            }
        }

        private String interpretDataType() {
            String data = this.getData();
            if (Metadata.isNumber(data)) {
                return "number";
            } else if (Metadata.isBoolean(data)) {
                return "boolean";
            } else if (Metadata.isObject(data)) {
                return "object";
            }
            return "string";
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Metadata {
        private String type;
        private Collection<String> options;

        private ObjectMapper mapper = new ObjectMapper();
        public Metadata(){}

        @JsonProperty("Type")
        public String getType() { return type; }

        @JsonProperty("Type")
        public void setType(String type) { this.type = type; }

        @JsonProperty("Options")
        public Collection<String> getOptions() { return options; }

        @JsonProperty("Options")
        public void setOptions(Collection<String> options) { this.options = options; }

        private boolean validateData(String data) {
            if (this.getType() == null) { return true; }
            switch (this.getType()) {
                case "number":
                    return isNumber(data);
                case "boolean":
                    return isBoolean(data);
                case "object":
                    return isObject(data);
                case "options":
                    return this.getOptions().contains(data);
                default:
                    return false;
            }
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

        private static boolean isObject(String str)
        {
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jNode = mapper.readTree(str);
                return jNode.isObject();
            } catch (IOException e) {
                return false;
            }
        }

        public static Collection<Node> applyMetadataOnJsonArrays(JsonNode configNodes, JsonNode metaNodes)
                throws JsonProcessingException {
            if (configNodes == null || !configNodes.isArray()) return null;

            ObjectMapper mapper = new ObjectMapper();

            Collection<Node> docNodes = new ArrayList<>();
            JsonNode metaNode = null;
            for (JsonNode jNode : configNodes) {
                Node docNode = new Node();

                docNode.setNode(jNode.get("Node").asText());

                // read data as text
                JsonNode dataNode = jNode.get("Data");
                if (dataNode != null) {
                    if (Metadata.isObject(dataNode.toString())) {
                        docNode.setData(dataNode.toString());
                    } else {
                        docNode.setData(dataNode.asText());
                    }
                }

                // sync with metadata
                Metadata metadata = null;
                if (metaNodes != null && metaNodes.isArray()) {
                    for (JsonNode thisMetaNode : metaNodes) {
                        if (thisMetaNode.isObject() && thisMetaNode.has("Node") &&
                                thisMetaNode.get("Node").asText().equals(docNode.getNode())){
                            metadata = mapper.treeToValue(thisMetaNode.get("Data"), Metadata.class);
                            metaNode = thisMetaNode;
                            break;
                        }
                    }
                }
                docNode.applySingleMetadata(metadata);

                // process children
                if (jNode.has("Children")) {
                    JsonNode metaChildren = metaNode != null ? metaNode.get("Children") : null;
                    docNode.setChildren(applyMetadataOnJsonArrays(jNode.get("Children"), metaChildren));
                }

                // add to the collection
                docNodes.add(docNode);
            }
            return docNodes;
        }

        @Override
        public String toString() {
            try {
                return mapper.writeValueAsString(this);
            } catch (JsonProcessingException e) {
                return this.toString();
            }
        }
    }
}
