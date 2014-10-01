package com.latticeengines.camille;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentMetadata;

public class DocumentUnitSerializerTestNG {
    
    @Test(groups = "unit")
    public void testSerializeDocument() {
        Document d = new Document(new String("foo"), new DocumentMetadata());
        try {
            byte[] data = DocumentSerializer.toByteArray(d);
            Document reconstituted = DocumentSerializer.toDocument(data);
            Assert.assertEquals(reconstituted, d);
        } catch (DocumentSerializationException e) {
            Assert.fail("Serialization failed: " + e);
        }
    }
 
}
