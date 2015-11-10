package com.latticeengines.dataplatform.infrastructure;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.interfaces.data.DataInterfacePublisher;
import com.latticeengines.camille.exposed.interfaces.data.DataInterfaceSubscriber;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class CamilleInfrastructureTestNG extends DataPlatformInfrastructureTestNGBase {

    @Test(groups = "infrastructure", enabled = true)
    public void testPubSub() throws Exception {

        String interfaceName = "interfaceName";
        CustomerSpace space = new CustomerSpace("ContractId", "TenantId", "SpaceId");
        DataInterfacePublisher pub = new DataInterfacePublisher(interfaceName, space);
        DataInterfaceSubscriber sub = new DataInterfaceSubscriber(interfaceName, space);

        Path relativePath = new Path("/CamilleTestNG.testPubSub");

        //Write
        Document pubDoc = new Document("Content");
        pub.publish(relativePath, pubDoc);

        //Read
        Document subDoc = sub.get(relativePath);
        Assert.assertEquals(pubDoc.getData(), subDoc.getData());

        //Cleanup
        pub.remove(relativePath);
        Assert.assertNull(sub.get(relativePath));
    }
}
