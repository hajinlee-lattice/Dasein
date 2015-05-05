package com.latticeengines.domain.exposed.camille;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DocumentDirectoryUnitTestNG {

    @Test(groups = "unit")
    public void testCreateDocumentDirectory() {
        DocumentDirectory dir = new DocumentDirectory(new Path("/root"));
        dir.add("/prop1", "value1");
        dir.add("/prop1/prop11", "value11");
        dir.add("/prop1/prop12", "value12");

        for (DocumentDirectory.Node node : dir.getChildren()) {
            Assert.assertEquals(node.getDocument(), new Document("value1"));
        }

        DocumentDirectory.Node node = dir.get("/prop1");
        Assert.assertEquals(node.getChildren().size(), 2);
    }

    @Test(groups = "unit")
    public void testCreateWithoutParent() {
        DocumentDirectory dir = new DocumentDirectory(new Path("/root"));
        dir.add("/prop1/prop11", "value11", true);
        dir.add("/prop1/prop12", "value12", true);

        DocumentDirectory.Node node = dir.get("/prop1");
        Assert.assertEquals(node.getChildren().size(), 2);
    }
}
