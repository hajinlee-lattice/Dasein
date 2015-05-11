package com.latticeengines.domain.exposed.admin;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SpaceConfiguration {

    private String sfdcOrgId;
    private String sandboxSfdcOrgId;
    private LatticeProduct product = LatticeProduct.LPA;
    private CRMTopology topology = CRMTopology.MARKETO;
    private String dlAddress;

    public SpaceConfiguration() {}

    public SpaceConfiguration(DocumentDirectory dir) {
        List<DocumentDirectory.Node> nodes = dir.getChildren();
        if (nodes == null || nodes.isEmpty())
            throw new RuntimeException(
                    new InstantiationException("Cannot parse the given DocumentDirectory to a SpaceConfiguration"));

        dir.makePathsLocal();

        for (DocumentDirectory.Node node : nodes) {
            if (node.getPath().toString().equals("/DL_Address")) {
                this.dlAddress = node.getDocument().getData();
            }
            if (node.getPath().toString().equals("/SFDC_OrgId")) {
                this.sfdcOrgId = node.getDocument().getData();
            }
            if (node.getPath().toString().equals("/SFDC_OrgId_Sandbox")) {
                this.sandboxSfdcOrgId = node.getDocument().getData();
            }
            if (node.getPath().toString().equals("/Product")) {
                this.product = LatticeProduct.fromName(node.getDocument().getData());
            }
            if (node.getPath().toString().equals("/Topology")) {
                this.topology = CRMTopology.fromName(node.getDocument().getData());
            }
        }

    }

    public DocumentDirectory toDocumentDirectory() {
        DocumentDirectory dir = new DocumentDirectory(new Path("/"));
        dir.add("/DL_Address", this.dlAddress);
        dir.add("/SFDC_OrgId", this.sfdcOrgId);
        dir.add("/SFDC_OrgId_Sandbox", this.sandboxSfdcOrgId);
        dir.add("/Product", this.product.getName());
        dir.add("/Topology", this.topology.getName());
        return dir;
    }

    public SerializableDocumentDirectory toSerializableDocumentDirectory() {
        return new SerializableDocumentDirectory(this.toDocumentDirectory());
    }


    @JsonProperty("DL_Address")
    public String getDlAddress() { return dlAddress; }

    @JsonProperty("DL_Address")
    public void setDlAddress(String dlAddress) { this.dlAddress = dlAddress; }

    @JsonProperty("SFDC_OrgId")
    public String getSfdcOrgId() { return sfdcOrgId; }

    @JsonProperty("SFDC_OrgId")
    public void setSfdcOrgId(String sfdcOrgId) { this.sfdcOrgId = sfdcOrgId; }

    @JsonProperty("SFDC_OrgId_Sandbox")
    public String getSandboxSfdcOrgId() { return sandboxSfdcOrgId; }

    @JsonProperty("SFDC_OrgId_Sandbox")
    public void setSandboxSfdcOrgId(String sandboxSfdcOrgId) { this.sandboxSfdcOrgId = sandboxSfdcOrgId; }

    @JsonProperty("Product")
    public LatticeProduct getProduct() { return product; }

    @JsonProperty("Product")
    public void setProduct(LatticeProduct product) { this.product = product; }

    @JsonProperty("Topology")
    public CRMTopology getTopology() { return topology; }

    @JsonProperty("Topology")
    public void setTopology(CRMTopology topology) { this.topology = topology; }
}
