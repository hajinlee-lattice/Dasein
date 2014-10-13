package com.latticeengines.camille;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentMetadata;

public class DocumentUnitTestNG {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    @Test(groups = "unit")
    public void testDeepCopy() throws Exception {
        String data = "foo";
        DocumentMetadata meta = new DocumentMetadata();
        Document doc = new Document(data, meta);
        Document docCopy = doc.deepCopy();
        Assert.assertFalse(doc == docCopy);
        Assert.assertTrue(doc.equals(docCopy));
        Assert.assertFalse(docCopy.getData() == data);
        Assert.assertTrue(docCopy.getData().equals(data));
        Assert.assertFalse(docCopy.getMetadata() == meta);
        Assert.assertTrue(docCopy.getMetadata().equals(meta));
        Assert.assertTrue(docCopy.getVersion() == docCopy.getVersion());
    }
}
