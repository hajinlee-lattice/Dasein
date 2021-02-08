package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.datacloud.manage.DataBlock.BLOCK_BASE_INFO;
import static com.latticeengines.domain.exposed.datacloud.manage.DataBlock.BLOCK_ENTITY_RESOLUTION;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.match.service.PrimeMetadataService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlock;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockLevel;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockMetadataContainer;
import com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn;

public class PrimeMetadataServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PrimeMetadataServiceImpl.class);

    @Inject
    private PrimeMetadataService primeMetadataService;

    // mainly just test parsing
    @Test(groups = "functional")
    private void testGetBlocks() {
        List<DataBlock> blocks = primeMetadataService.getDataBlocks();
        Assert.assertEquals(blocks.size(), 18);
        DataBlock compInfoBlock = blocks.stream() //
                .filter(b -> "companyinfo".equals(b.getBlockId())).findFirst().orElse(null);
        Assert.assertNotNull(compInfoBlock);
    }

    // mainly just test parsing
    @Test(groups = "functional")
    private void testGetDataBlockMetadata() {
        DataBlockMetadataContainer container = primeMetadataService.getDataBlockMetadata();
        Assert.assertNotNull(container);
        Assert.assertTrue(container.getBlocks().containsKey(BLOCK_BASE_INFO));
        Assert.assertTrue(container.getBlocks().containsKey(BLOCK_ENTITY_RESOLUTION));
    }

    @Test(groups = "functional")
    private void testGetPrimeColumns() {
        List<String> lst = Arrays.asList( //
                "duns_number", //
                "primaryname", //
                "non_exist_element_1", //
                "non_exist_element_2");
        List<PrimeColumn> primeColumns = primeMetadataService.getPrimeColumns(lst);
        Assert.assertEquals(primeColumns.size(), 2);
    }

    @Test(groups = "functional")
    private void testContainsExample() {
        List<String> lst = Arrays.asList( //
                "duns_number", //
                "primaryname", //
                "banks_name", //
                "summary_text", //
                "latestfin_dataprovider_desc", //
                "non_exist_element_1", //
                "non_exist_element_2");
        List<PrimeColumn> primeColumns = primeMetadataService.getPrimeColumns(lst);
        Assert.assertEquals(primeColumns.size(), 5);
        for (PrimeColumn pc : primeColumns) {
            switch (pc.getJsonPath()) {
            case "organization.duns":
                Assert.assertEquals(pc.getExample(), "804735132",
                        String.format("For JSON path for %s ", pc.getJsonPath()));
                break;
            case "organization.primaryName":
                Assert.assertEquals(pc.getExample(), "GORMAN MANUFACTURING COMPANY, INC.",
                        String.format("For JSON path for %s ", pc.getJsonPath()));
                break;
            case "organization.banks.name":
                Assert.assertEquals(pc.getExample(), "Bank of My Country",
                        String.format("JSON path for %s ", pc.getJsonPath()));
                break;
            case "organization.latestFinancials.dataProvider.description":
                Assert.assertNotEquals(pc.getExample(), "Her Majesty's Secret Service");
                Assert.assertEquals(pc.getExample(), "Her Majesty's Revenue and Customs",
                        String.format("JSON path for %s ", pc.getJsonPath()));
                break;
            case "organization.summary.text":
                Assert.assertEquals(pc.getExample(), "The popularity of the widget fueled AB Corp's strong revenue and "
                        + "earnings growth in the past decade. The company's revenue and profit hit all-time highs in 2017, "
                        + "coming in at $234 million and $53 million, respectively. Revenue and earnings fell back in 2018 "
                        + "with weaker widget sales.", String.format("JSON path for %s ", pc.getJsonPath()));
                break;
            default:
                Assert.fail(String.format("JSON Path %s does not correspond to a known PrimeColumn", pc.getJsonPath()));
            }
        }
    }

    @Test(groups = "functional", dataProvider = "blockElements")
    private void testResolveBlocks(List<String> elementIds, int expectedBlocks) {
        List<PrimeColumn> primeColumns = primeMetadataService.getPrimeColumns(elementIds);
        Map<String, List<PrimeColumn>> columnsByBlock = primeMetadataService.divideIntoBlocks(primeColumns);
        Assert.assertEquals(columnsByBlock.size(), expectedBlocks);
    }

    @Test(groups = "functional")
    private void filterFinancialDataBlocks() {
        List<DataBlock> dataBlocks = getTestDataBlocks();
        Assert.assertTrue(dataBlocks.size() == 3);

        List<DataBlock> filteredDataBlocks = PrimeMetadataServiceImpl.filterFinancialDataBlockLevels(dataBlocks);
        Assert.assertTrue(filteredDataBlocks.size() == 3);

        DataBlock financialBlock = null;
        for (DataBlock block : filteredDataBlocks) {
            if (DataBlock.Id.companyfinancials.equals(block.getBlockId())) {
                financialBlock = block;
            }
        }

        Assert.assertNotNull(financialBlock);

        Assert.assertTrue(financialBlock.getLevels().size() == 1);
    }

    private List<DataBlock> getTestDataBlocks() {
        List<DataBlock> blocks = new ArrayList<>();

        DataBlock.Level l1 = new DataBlock.Level(DataBlockLevel.L1);
        DataBlock.Level l2 = new DataBlock.Level(DataBlockLevel.L2);
        DataBlock.Level l3 = new DataBlock.Level(DataBlockLevel.L3);

        DataBlock dataBlock1 = new DataBlock(DataBlock.Id.baseinfo, Arrays.asList(l1, l2, l3));
        DataBlock dataBlock2 = new DataBlock(DataBlock.Id.companyinfo, Arrays.asList(l1, l2, l3));
        DataBlock dataBlock3 = new DataBlock(DataBlock.Id.companyfinancials, Arrays.asList(l1, l2, l3));

        blocks.add(dataBlock1);
        blocks.add(dataBlock2);
        blocks.add(dataBlock3);

        return blocks;
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
                "dnbassessment_supplierstabilityindexscore_failurerate");
        List<String> lst3 = Arrays.asList( //
                "non_exist_element_1", //
                "non_exist_element_2");
        List<String> lst4 = Arrays.asList( //
                "duns_number", //
                "primaryname", //
                "non_exist_element_1", //
                "non_exist_element_2");
        return new Object[][] { { lst1, 2 }, //
                { lst2, 2 }, //
                { lst3, 0 }, //
                { lst4, 1 }, //
        };
    }

    // test that all entityresolution elements are present and contain the expected data
    @Test(groups = "functional")
    private void testEntityResolutionBlockElements() {
        List<DataBlock> blocks = primeMetadataService.getDataBlocks();
        System.out.println(JsonUtils.pprint(blocks));
        Assert.assertEquals(blocks.size(), 18, String.format("Incorrect num of data blocks.  Actual blocks %s", JsonUtils.pprint(getTestDataBlocks())));
        DataBlock entityResolutionBlock = blocks.stream() //
                .filter(b -> "entityresolution".equals(b.getBlockId())).findFirst().orElse(null);
        Assert.assertNotNull(entityResolutionBlock);
        List<DataBlock.Level> levels = entityResolutionBlock.getLevels();
        DataBlock.Level level = levels.get(0);
        Assert.assertNotNull(level);

        DataBlock.Level levelOne = entityResolutionBlock.getLevels().stream().filter( l -> DataBlockLevel.L1.equals(l.getLevel())).findFirst().orElse(null);
        Assert.assertNotNull(levelOne);
        List<DataBlock.Element> elementList = levelOne.getElements();
        Assert.assertNotNull(elementList);
        Assert.assertEquals(elementList.size(), 9);

        // Check some of the elements in this data block
        DataBlock.Element matchedDuns = elementList.stream().filter(element -> element.getElementId().equals("MatchedDuns")).findFirst().orElse(null);
        Assert.assertNotNull(matchedDuns);
        Assert.assertEquals(matchedDuns.getDescription(), "The D-U-N-S Number, assigned by Dun & Bradstreet, is an identification number that uniquely identifies the entity in accordance with the Data Universal Numbering System (D-U-N-S).");
        Assert.assertEquals(matchedDuns.getDataType(), "String");

        DataBlock.Element iso2CountryCode = elementList.stream().filter(element -> element.getElementId().equals("MatchIso2CountryCode")).findFirst().orElse(null);
        Assert.assertNotNull(iso2CountryCode);
        Assert.assertEquals(iso2CountryCode.getDataType(), "String");
        Assert.assertEquals( iso2CountryCode.getDescription() ,"The two-letter country code, defined by the International Organization for Standardization (ISO) ISO 3166-1 scheme identifying the country/market in which this address is located.");

        DataBlock.Element confidenceCode = elementList.stream().filter(element -> element.getElementId().equals("ConfidenceCode")).findFirst().orElse(null);
        Assert.assertNotNull(confidenceCode);
        Assert.assertEquals(confidenceCode.getDataType(), "Integer");
        Assert.assertEquals(confidenceCode.getDescription(), "A numeric value from 1 (low) up to 10 (high) indicating the level of certainty at which this possible candidate was included in this result set.");

    }


}
