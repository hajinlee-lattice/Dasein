package com.latticeengines.domain.exposed.admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SpaceConfiguration {

    private LatticeProduct product;
    private List<LatticeProduct> products = Collections.emptyList();
    private CRMTopology topology;
    private String dlAddress;

    private static ObjectMapper mapper = new ObjectMapper();

    public SpaceConfiguration() {
    }

    @SuppressWarnings("unchecked")
    public SpaceConfiguration(DocumentDirectory dir) {
        List<DocumentDirectory.Node> nodes = dir.getChildren();
        if (nodes == null || nodes.isEmpty())
            throw new RuntimeException(new InstantiationException(
                    "Cannot parse the given DocumentDirectory to a SpaceConfiguration"));

        dir.makePathsLocal();

        for (DocumentDirectory.Node node : nodes) {
            if (node.getPath().toString().equals("/DL_Address")) {
                this.dlAddress = node.getDocument().getData();
            }
            if (node.getPath().toString().equals("/Product")) {
                this.product = LatticeProduct.fromName(node.getDocument().getData());
            }
            if (node.getPath().toString().equals("/Products")) {
                try {
                    List<String> products = mapper.readValue(node.getDocument().getData(), List.class);
                    this.products = new ArrayList<>();
                    for (String product : products) {
                        this.products.add(LatticeProduct.fromName(product));
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if (node.getPath().toString().equals("/Topology")) {
                this.topology = CRMTopology.fromName(node.getDocument().getData());
            }
        }
        // TODO
        // For the email servicel also consider the content of the email to be
        // applied to LPA as well
        // if the node of products is empty, then assign it to be LPA by
        // default. This is for backward compatibility
        // concern

        // also add logic in front end to prevent uncheck two boxes before
        // clikcing install

    }

    public DocumentDirectory toDocumentDirectory() {
        try {
            DocumentDirectory dir = new DocumentDirectory(new Path("/"));
            dir.add("/DL_Address", this.dlAddress);
            if (this.product != null) {
                dir.add("/Product", this.product.getName());
            }
            dir.add("/Products", mapper.writeValueAsString(this.products));
            if (this.topology != null) {
                dir.add("/Topology", this.topology.getName());
            }
            return dir;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public SerializableDocumentDirectory toSerializableDocumentDirectory() {
        return new SerializableDocumentDirectory(this.toDocumentDirectory());
    }

    @JsonProperty("DL_Address")
    public String getDlAddress() {
        return dlAddress;
    }

    @JsonProperty("DL_Address")
    public void setDlAddress(String dlAddress) {
        this.dlAddress = dlAddress;
    }

    @JsonProperty("Product")
    public LatticeProduct getProduct() {
        return product;
    }

    @JsonProperty("Product")
    public void setProduct(LatticeProduct product) {
        this.product = product;
    }

    @JsonProperty("Products")
    public List<LatticeProduct> getProducts() {
        return products;
    }

    @JsonProperty("Products")
    public void setProducts(List<LatticeProduct> products) {
        this.products = products;
    }

    @JsonProperty("Topology")
    public CRMTopology getTopology() {
        return topology;
    }

    @JsonProperty("Topology")
    public void setTopology(CRMTopology topology) {
        this.topology = topology;
    }
}
