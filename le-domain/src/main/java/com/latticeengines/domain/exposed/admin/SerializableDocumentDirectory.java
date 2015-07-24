package com.latticeengines.domain.exposed.admin;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.validator.routines.EmailValidator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SerializableDocumentDirectory implements Iterable<SerializableDocumentDirectory.Node> {
    private static final Log LOGGER = LogFactory.getLog(SerializableDocumentDirectory.class);

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
                docDir.add(nodePath, new Document(entry.getValue()), true);
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
            } else {
                result.put(node.getPath().toString(), "");
            }
        }

        return result;
    }

    public void applyMetadata (DocumentDirectory metadataDirectory) {
        applyMetadataTemplate(metadataDirectory, false);
    }

    public void applyMetadataIgnoreOptionsValidation (DocumentDirectory metadataDirectory) {
        applyMetadataTemplate(metadataDirectory, true);
    }

    public void applyMetadataTemplate (DocumentDirectory metadataDirectory, boolean ignoreOptions) {
        if (metadataDirectory != null && this.getNodes() != null) {
            // apply metadata to nodes
            for (Node node : this.getNodes()) {
                node.ignoreOptionsValidateion = ignoreOptions;
                DocumentDirectory.Node metaNode = metadataDirectory.get("/" + node.getNode());
                node.applyMetadata(metaNode);
            }
        }
    }

    @JsonIgnore
    public DocumentDirectory getMetadataAsDirectory() {
        Path rootPath = new Path("/");
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
        if (serializedDir.getNodes() != null) {
            deserializeNodes(dir, serializedDir.getNodes(), "");
        }
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

    public List<SelectableConfigurationField> findSelectableFields() {
        return findSelectableFields(false);
    }

    public List<SelectableConfigurationField> findSelectableFields(boolean includeDynamicOptions) {
        List<SelectableConfigurationField> optFields = new ArrayList<>();
        if (this.getNodes() != null && !this.getNodes().isEmpty()) {
            String parent = "";
            for(Node node : this.getNodes()) {
                optFields.addAll(node.findSelectableFields(parent, includeDynamicOptions));
            }
        }
        return optFields;
    }

    @Override
    public String toString() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            String string = mapper.writeValueAsString(this);
            string = string.replace("\\\"", "\"");
            string = string.replace("\\\\", "\\");
            return string;
        } catch (IOException e) {
            return "Failed to serialize " + super.toString();
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

    @JsonIgnore
    public Iterator<Node> getBreathFirstIterator() {
        return new BreathFirstIterator(this.getNodes());
    }

    @JsonIgnore
    public Iterator<Node> getDepthFirstIterator() {
        return new DepthFirstIterator(this.getNodes());
    }

    public Iterator<Node> iterator() { return new DepthFirstIterator(this.getNodes()); }

    public static class BreathFirstIterator implements Iterator<Node> {

        private Queue<Node> queue = new ArrayDeque<>();

        public BreathFirstIterator(Collection<Node> roots) {
            if (roots != null) {
                for (Node root: roots) {
                    root.path = new Path("/" + root.getNode());
                    queue.add(root);
                }
            }
        }

        @Override
        public void remove() { throw new UnsupportedOperationException(); }

        @Override
        public Node next() {
            Node front = queue.poll();
            Path rootPath = front.path;
            if (front.getChildren() != null) {
                for (Node child: front.getChildren()) {
                    child.path = rootPath.append(child.getNode());
                    queue.add(child);
                }
            }
            return front;
        }

        @Override
        public boolean hasNext() { return !queue.isEmpty(); }
    }

    public static class DepthFirstIterator implements Iterator<Node> {

        private Stack<Node> stack = new Stack<>();

        public DepthFirstIterator(Collection<Node> roots) {
            if (roots != null) {
                for (Node root: roots) {
                    root.path = new Path("/" + root.getNode());
                    stack.push(root);
                }
            }
        }

        @Override
        public void remove() { throw new UnsupportedOperationException(); }

        @Override
        public Node next() {
            Node front = stack.pop();
            Path rootPath = front.path;
            if (front.getChildren() != null) {
                for (Node child: front.getChildren()) {
                    child.path = rootPath.append(child.getNode());
                    stack.push(child);
                }
            }
            return front;
        }

        @Override
        public boolean hasNext() { return !stack.isEmpty(); }
    }

    public Node getNodeAtPath(Path path) {
        for (Node node: this) {
            if (node.path.equals(path)) {
                return node;
            }
        }
        return null;
    }

    public Node getNodeAtPath(String path) { return getNodeAtPath(new Path(path)); }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Node {

        private String node;
        private String data;
        private Metadata metadata;
        private int version = -1;
        private Collection<Node> children;

        @JsonIgnore
        public Path path;
        @JsonIgnore
        public boolean ignoreOptionsValidateion;

        public Node() { }

        public Node(DocumentDirectory.Node documentNode) {
            this.node = documentNode.getPath().getSuffix();
            this.data = documentNode.getDocument().getData();
            if (this.data.equals("") && documentNode.getChildren() != null && !documentNode.getChildren().isEmpty()) {
                this.data = null;
            }
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

        public void applyMetadata(DocumentDirectory.Node metaNode) {
            if (metaNode == null) return;

            if (this.getData() != null) {
                Metadata metadataProvided = null;
                ObjectMapper mapper = new ObjectMapper();

                try {
                    metadataProvided = mapper.readValue(metaNode.getDocument().getData(), Metadata.class);
                } catch (NullPointerException | IOException e) {
                    //ignore
                }

                if (metadataProvided != null) {
                    this.applySingleMetadata(metadataProvided);
                }
            }

            // apply metadata to children
            if (this.getChildren() != null) {
                for (Node child : this.getChildren()) {
                    child.ignoreOptionsValidateion = this.ignoreOptionsValidateion;
                    DocumentDirectory.Node childMetaNode = metaNode.getChild(child.getNode());
                    child.applyMetadata(childMetaNode);
                }
            }
        }

        public void applySingleMetadata(Metadata metadata) {
            if (this.getData() == null) return;

            if (metadata != null && metadata.getType() != null && !metadata.getType().equals("")) {
                if (metadata.validateDataTemplate(this.getData(), ignoreOptionsValidateion)) {
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

        public void writeMetadataToDir(DocumentDirectory dir, String parentPath) {
            String nodePath = parentPath + "/" + this.node;

            if (this.metadata != null && this.metadata.getType() != null
                    && !(this.metadata.getType().equals(""))) {
                dir.add(nodePath, this.metadata.toString());
            } else {
                dir.add(nodePath, "");
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
            } else if (Metadata.isArray(data)) {
                return "array";
            }
            return "string";
        }


        public List<SelectableConfigurationField> findSelectableFields(String parent, boolean includeDynamicOptions) {
            List<SelectableConfigurationField> optFields = new ArrayList<>();
            Metadata metadata = this.getMetadata();
            if (metadata != null && metadata.getType().equals("options") &&
                    (includeDynamicOptions || metadata.isDynamicOptions() == null || !metadata.isDynamicOptions())) {
                if (this.getMetadata().validateDataIgnoreOptions(this.getData())) {
                    SelectableConfigurationField field = new SelectableConfigurationField();
                    field.setNode(parent + "/" + this.getNode());
                    field.setDefaultOption(this.getData());
                    field.setOptions(new ArrayList<>(this.getMetadata().getOptions()));
                    optFields.add(field);
                } else {
                    LOGGER.warn("Found an invalid optional configuration field at " + this.getNode());
                }
            }
            if (this.getChildren() != null && !this.getChildren().isEmpty()) {
                for (Node child : this.getChildren()) {
                    optFields.addAll(child.findSelectableFields(parent + "/" + this.getNode(), includeDynamicOptions));
                }
            }
            return optFields;
        }

    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Metadata {
        private static EmailValidator emailValidator = EmailValidator.getInstance();

        private String type;
        private Collection<String> options;
        private Boolean dynamicOptions = null;
        private String helper;
        private Boolean required = null;
        private DerivedField derived;
        private Boolean readonly = null;

        private ObjectMapper mapper = new ObjectMapper();
        public Metadata() { }

        @JsonProperty("Type")
        public String getType() { return type; }

        @JsonProperty("Type")
        public void setType(String type) { this.type = type; }

        @JsonProperty("Options")
        public Collection<String> getOptions() { return options; }

        @JsonProperty("Options")
        public void setOptions(Collection<String> options) { this.options = options; }

        @JsonProperty("DynamicOptions")
        public Boolean isDynamicOptions() { return dynamicOptions; }

        @JsonProperty("DynamicOptions")
        public void setDynamicOptions(Boolean dynamicOptions) { this.dynamicOptions = dynamicOptions; }

        @JsonProperty("Required")
        public Boolean isRequired() { return required; }

        @JsonProperty("Required")
        public void setRequired(Boolean required) { this.required = required; }

        @JsonProperty("Readonly")
        public Boolean isReadonly() { return readonly; }

        @JsonProperty("Readonly")
        public void setReadonly(Boolean readonly) { this.readonly = readonly; }

        @JsonProperty("Helper")
        public String getHelper() { return helper; }

        @JsonProperty("Helper")
        public void setHelper(String helper) { this.helper = helper; }

        @JsonProperty("Derived")
        public DerivedField getDerived() { return derived; }

        @JsonProperty("Derived")
        public void setDerived(DerivedField derived) { this.derived = derived; }

        private boolean validateData(String data) {
            return validateDataTemplate(data, false);
        }

        private boolean validateDataIgnoreOptions(String data) {
            return validateDataTemplate(data, true);
        }

        private boolean validateDataTemplate(String data, boolean ignoreOptions) {
            if (this.getType() == null) { return true; }
            switch (this.getType()) {
                case "number":
                    return isNumber(data);
                case "boolean":
                    return isBoolean(data);
                case "object":
                    return true; //isObject(data);
                case "array":
                    return true; // isArray(data);
                case "options":
                    if (ignoreOptions) return true;
                    if (this.dynamicOptions != null) {
                        return this.isDynamicOptions() || this.getOptions().contains(data);
                    } else {
                        return this.getOptions().contains(data);
                    }
                case "email":
                    return emailValidator.isValid(data);
                case "path":
                    return isPath(data);
                case "string":
                case "password":
                    return true;
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

        private static boolean isPath(String str)
        {
            try {
                new Path(str);
                return true;
            } catch (Exception e) {
                return false;
            }
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

        private static boolean isArray(String str)
        {
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jNode = mapper.readTree(str);
                return jNode.isArray();
            } catch (IOException e) {
                return false;
            }
        }

        public static Collection<Node> applyMetadataOnJsonArrays(JsonNode configNodes, JsonNode metaNodes)
                throws JsonProcessingException {
            if (configNodes == null || !configNodes.isArray()) return null;

            ObjectMapper mapper = new ObjectMapper();

            Collection<Node> docNodes = new ArrayList<>();
            for (JsonNode jNode : configNodes) {
                Node docNode = new Node();

                docNode.setNode(jNode.get("Node").asText());

                // read data as text
                JsonNode dataNode = jNode.get("Data");
                boolean mustBeString = false;
                if (dataNode != null) {
                    String data;
                    if (Metadata.isObject(dataNode.toString()) || Metadata.isArray(dataNode.toString())) {
                        data = dataNode.toString();
                    }else if (dataNode.toString().startsWith("\"") && dataNode.toString().endsWith("\"")) {
                        data = unescapeDataText(dataNode.toString().substring(1, dataNode.toString().length() - 1));
                        mustBeString = isNumber(dataNode.asText()) || isBoolean(dataNode.asText());
                    } else {
                        data = unescapeDataText(dataNode.asText());
                    }
                    docNode.setData(data);
                }

                // sync with metadata
                Metadata metadata = null;
                JsonNode metaNode = null;
                if (metaNodes != null && metaNodes.isArray()) {
                    for (JsonNode thisMetaNode : metaNodes) {
                        if (thisMetaNode.isObject() && thisMetaNode.has("Node") &&
                                thisMetaNode.get("Node").asText().equals(docNode.getNode())){
                            if (thisMetaNode.has("Data")) {
                                metadata = mapper.treeToValue(thisMetaNode.get("Data"), Metadata.class);
                            }
                            metaNode = thisMetaNode;
                            break;
                        }
                    }
                }
                if (metaNode == null && mustBeString) {
                    metadata = new Metadata();
                    metadata.setType("string");
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

        private static String unescapeDataText(String data) {
            return StringEscapeUtils.unescapeJson(data);
        }
    }
}
