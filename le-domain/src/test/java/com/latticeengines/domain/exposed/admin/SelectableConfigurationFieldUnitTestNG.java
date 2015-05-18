package com.latticeengines.domain.exposed.admin;

import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;

public class SelectableConfigurationFieldUnitTestNG {

    SerializableDocumentDirectory dir;

    @BeforeClass(groups = "unit")
    public void setup() {
        DocumentDirectory configDir = new DocumentDirectory(new Path("/"));
        configDir.add("/prop", "option1");
        configDir.add("/dynamic", "option1");
        dir = new SerializableDocumentDirectory(configDir);

        DocumentDirectory metaDir = new DocumentDirectory(new Path("/"));
        metaDir.add("/prop", "{\"Type\":\"options\",\"Options\":[\"option1\",\"option2\",\"option3\"]}");
        metaDir.add("/dynamic", "{\"Type\":\"options\",\"Options\":[], \"DynamicOptions\":true}");

        dir.applyMetadata(metaDir);
    }

    @Test(groups = "unit")
    public void testPatch() {
        SelectableConfigurationField field = new SelectableConfigurationField();
        field.setNode("prop");
        field.setOptions(Arrays.asList("option1", "option2", "option3", "option4"));
        field.patch(dir);

        SerializableDocumentDirectory.Node node = dir.getNodeAtPath("/prop");
        Assert.assertEquals(node.getMetadata().getOptions().size(), 4);
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void testPatchNonExistingNode() {
        SelectableConfigurationField field = new SelectableConfigurationField();
        field.setNode("nope");
        field.setOptions(Arrays.asList("option1", "option2", "option3", "option4"));
        field.patch(dir);
    }

    @Test(groups = "unit")
    public void testPatchWithNullOptions() {
        SelectableConfigurationField field = new SelectableConfigurationField();
        field.setNode("prop");
        field.patch(dir);

        SerializableDocumentDirectory.Node node = dir.getNodeAtPath("/prop");
        Assert.assertEquals(node.getMetadata().getOptions().size(), 0);
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void testPatchDyanmicOptions() {
        SelectableConfigurationField field = new SelectableConfigurationField();
        field.setNode("dynamic");
        field.setOptions(Arrays.asList("option1", "option2", "option3", "option4"));
        field.patch(dir);
    }
}
