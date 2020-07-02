package com.latticeengines.datacloud.match.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.service.PrimeMetadataService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlock;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockMetadataContainer;

public class PrimeMetadataServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    @Inject
    private PrimeMetadataService primeMetadataService;

    // mainly just test parsing
    @Test(groups = "functional")
    private void testGetBlocks() {
         List<DataBlock> blocks = primeMetadataService.getDataBlocks();
         // System.out.println(JsonUtils.pprint(blocks));
         Assert.assertEquals(blocks.size(), 11);
         DataBlock compInfoBlock = blocks.stream() //
                 .filter(b -> "companyinfo".equals(b.getBlockId())).findFirst().orElse(null);
         Assert.assertNotNull(compInfoBlock);
    }

    // mainly just test parsing
    @Test(groups = "functional")
    private void testGetDataBlockMetadata() {
        DataBlockMetadataContainer container = primeMetadataService.getDataBlockMetadata();
        // System.out.println(JsonUtils.pprint(container));
        Assert.assertNotNull(container);
    }

    // mainly just test parsing
    @Test(groups = "functional")
    private void testGetBaseEntitlement() {
        DataBlockEntitlementContainer container = primeMetadataService.getBaseEntitlement();
        // System.out.println(JsonUtils.pprint(container));
        Assert.assertNotNull(container);
    }

}
