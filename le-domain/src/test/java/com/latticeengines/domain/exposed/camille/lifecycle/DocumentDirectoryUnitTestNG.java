package com.latticeengines.domain.exposed.camille.lifecycle;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;

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
}
