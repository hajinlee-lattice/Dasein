package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.datacloud.manage.DataBlock.BLOCK_BASE_INFO;
import static com.latticeengines.domain.exposed.datacloud.manage.DataBlock.BLOCK_ENTITY_RESOLUTION;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.match.service.PrimeMetadataService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlock;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockMetadataContainer;
import com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn;

public class PrimeMetadataServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    @Inject
    private PrimeMetadataService primeMetadataService;

    // mainly just test parsing
    @Test(groups = "functional")
    private void testGetBlocks() {
         List<DataBlock> blocks = primeMetadataService.getDataBlocks();
         System.out.println(JsonUtils.pprint(blocks));
         Assert.assertEquals(blocks.size(), 13);
         DataBlock compInfoBlock = blocks.stream() //
                 .filter(b -> "companyinfo".equals(b.getBlockId())).findFirst().orElse(null);
         Assert.assertNotNull(compInfoBlock);
    }

    // mainly just test parsing
    @Test(groups = "functional")
    private void testGetDataBlockMetadata() {
        DataBlockMetadataContainer container = primeMetadataService.getDataBlockMetadata();
        // System.out.println(JsonUtils.serialize(container));
        Assert.assertNotNull(container);
        Assert.assertTrue(container.getBlocks().containsKey(BLOCK_BASE_INFO));
        Assert.assertTrue(container.getBlocks().containsKey(BLOCK_ENTITY_RESOLUTION));
    }

    @Test(groups ="functional")
    private void testGetPrimeColumns() {
        List<String> lst = Arrays.asList( //
                "duns_number", //
                "primaryname", //
                "non_exist_element_1", //
                "non_exist_element_2"
        );
        List<PrimeColumn> primeColumns = primeMetadataService.getPrimeColumns(lst);
        Assert.assertEquals(primeColumns.size(), 2);
    }

    @Test(groups = "functional", dataProvider = "blockElements")
    private void testResolveBlocks(List<String> elementIds, int expectedBlocks) {
        Set<String> blockIds = primeMetadataService.getBlocksContainingElements(elementIds);
        Assert.assertEquals(blockIds.size(), expectedBlocks);
    }

    @DataProvider(name = "blockElements")
    private Object[][] provideBlockElements() {
        List<String> lst1 = Arrays.asList( //
                "duns_number", //
                "primaryname", //
                "tradestylenames_name", //
                "primaryaddr_street_line1", //
                "primaryaddr_street_line2", //
                "primaryaddr_addrlocality_name", //
                "primaryaddr_addrregion_name", //
                "primaryaddr_postalcode", //
                "primaryaddr_country_name", //
                "telephone_telephonenumber", //
                "primaryindcode_ussicv4" //
        );
        List<String> lst2 = Arrays.asList( //
                // financialstrengthinsight_L1_v1, thirdpartyriskinsight_L1_v1
                "dnbassessment_delinquencyscore_classscore", //
                "dnbassessment_delinquencyscore_scoredate", //
                // financialstrengthinsight_L2_v1, thirdpartyriskinsight_L2_v1
                "dnbassessment_failurescore_nationalpercentile",
                // financialstrengthinsight_L3_v1
                "delinquencyscorenorms_calculationtimestamp",
                // financialstrengthinsight_L4_v1
                "dnbassessment_failurescorehistory_rawscore",
                // thirdpartyriskinsight_L3_v1
                "dnbassessment_supplierstabilityindexscore_failurerate"
        );
        List<String> lst3 = Arrays.asList( //
                "non_exist_element_1", //
                "non_exist_element_2"
        );
        List<String> lst4 = Arrays.asList( //
                "duns_number", //
                "primaryname", //
                "non_exist_element_1", //
                "non_exist_element_2"
        );
        return new Object[][] {
                { lst1, 1 }, //
                { lst2, 2 }, //
                { lst3, 1 }, //
                { lst4, 1 }, //
        };
    }

}
