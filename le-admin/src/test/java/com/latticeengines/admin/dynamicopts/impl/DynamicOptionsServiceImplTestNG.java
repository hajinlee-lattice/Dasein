package com.latticeengines.admin.dynamicopts.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.admin.dynamicopts.DynamicOptionsService;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationDocument;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-admin-context.xml" })
public class DynamicOptionsServiceImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private DynamicOptionsService dynamicOptionsService;

    @Test(groups = "functional")
    public void testDynamicBindingOnSerializableDocumentDirectory() {
        Map<String, String> flatDir = new HashMap<>();
        flatDir.put("/Topology", "Marketo");
        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(flatDir);
        sDir.setRootPath("/" + LatticeComponent.spaceConfigNode);

        dynamicOptionsService.bind(sDir);

        for (SerializableDocumentDirectory.Node node : sDir.getNodes()) {
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

        dynamicOptionsService.bind(doc);

        for (SelectableConfigurationField node : doc.getNodes()) {
            if (node.getNode().equals("/Topology")) {
                Assert.assertEquals(node.getOptions().size(), CRMTopology.values().length);
            } else {
                Assert.assertEquals(node.getOptions().size(), 1);
            }
        }
    }
}
