package com.latticeengines.camille;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.Document;

public class DocumentSerializerUnitTestNG {

    @Test(groups = "unit")
    public void testSerializeDocument() {
        Document d = new Document(new String("foo"));
        byte[] data = d.getData().getBytes();
        Document reconstituted = new Document(new String(data));
        Assert.assertEquals(reconstituted, d);
    }
}
