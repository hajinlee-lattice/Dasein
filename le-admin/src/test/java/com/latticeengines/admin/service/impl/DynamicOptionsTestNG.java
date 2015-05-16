package com.latticeengines.admin.service.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationDocument;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-admin-context.xml" })
public class DynamicOptionsTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private DynamicOptions dynamicOptions;

    @Test(groups = "functional")
    public void checkTopologyOptions() {
        Path topologyPath = PathBuilder.buildServiceConfigSchemaPath(CamilleEnvironment.getPodId(),
                LatticeComponent.spaceConfigNode);
        topologyPath = topologyPath.append(new Path("/Topology"));
        Set<String> topologyOptions = new HashSet<>(dynamicOptions.getOpitions(topologyPath));
        Assert.assertEquals(topologyOptions, new HashSet<>(CRMTopology.getNames()));
    }

    @Test(groups = "functional")
    public void checkProductOptions() {
        Path topologyPath = PathBuilder.buildServiceConfigSchemaPath(CamilleEnvironment.getPodId(),
                LatticeComponent.spaceConfigNode);
        topologyPath = topologyPath.append(new Path("/Product"));
        Set<String> topologyOptions = new HashSet<>(dynamicOptions.getOpitions(topologyPath));
        Assert.assertEquals(topologyOptions, new HashSet<>(LatticeProduct.getNames()));
    }

    @Test(groups = "functional")
    public void testDynamicBindingOnSerializableDocumentDirectory() {
        Map<String, String> flatDir = new HashMap<>();
        flatDir.put("/Topology", "Marketo");
        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(flatDir);
        Path rootPath = PathBuilder.buildServiceConfigSchemaPath(CamilleEnvironment.getPodId(),
                LatticeComponent.spaceConfigNode);
        sDir.setRootPath(rootPath.toString());

        dynamicOptions.applyDynamicBinding(sDir);

        for (SerializableDocumentDirectory.Node node: sDir.getNodes()){
            Assert.assertEquals(node.getData(), "Marketo");
            SerializableDocumentDirectory.Metadata metadata = node.getMetadata();
            Assert.assertNotNull(metadata);
            Assert.assertEquals(metadata.getOptions().size(), CRMTopology.values().length);
        }
    }

    @Test(groups = "functional")
    public void testDynamicBindingOnSelectableConfigurationDocument() {
        SelectableConfigurationField field = new SelectableConfigurationField();
        field.setDefaultOption("Marketo");
        field.setOptions(Collections.singletonList("Marketo"));
        field.setNode("/Topology");

        SelectableConfigurationField field2 = new SelectableConfigurationField();
        field2.setDefaultOption("Somevalue");
        field2.setOptions(Collections.singletonList("Somevalue"));
        field2.setNode("/Somenode");

        SelectableConfigurationDocument doc = new SelectableConfigurationDocument();
        doc.setComponent(LatticeComponent.spaceConfigNode);
        doc.setNodes(Collections.singletonList(field));

        dynamicOptions.applyDynamicBinding(doc);

        for (SelectableConfigurationField node: doc.getNodes()){
            if (node.getNode().equals("/Topology")) {
                Assert.assertEquals(node.getOptions().size(), CRMTopology.values().length);
            } else {
                Assert.assertEquals(node.getOptions().size(), 1);
            }
        }
    }
}
