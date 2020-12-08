package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.datacloud.manage.DataBlockLevel.L1;
import static com.latticeengines.domain.exposed.datacloud.manage.DataBlockLevel.L2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.manage.DataBlockElement;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockLevel;
import com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn;

public class PrimeMetadataServiceImplUnitTestNG {

    @Test(groups = "unit", dataProvider = "blockElements")
    private void testConsolidateBlocks(List<DataBlockElement> blockElements, int expectedBlocks) {
         Map<String, List<String>> blockIds = PrimeMetadataServiceImpl.consolidateBlocks(blockElements);
         Assert.assertEquals(blockIds.size(), expectedBlocks);
    }

    @DataProvider(name = "blockElements")
    private Object[][] provideBlockElements() {
        List<DataBlockElement> lst1 = Arrays.asList( //
                getBlockElement(1, L1,1), //
                getBlockElement(2, L1,2 ), //
                getBlockElement(3, L1,3 ), //
                getBlockElement(4, L1,4 ) //
        );
        List<DataBlockElement> lst2 = Arrays.asList( //
                getBlockElement(1, L1,1), //
                getBlockElement(1, L1,2), //
                getBlockElement(2, L1,2 ), //
                getBlockElement(3, L1,3 ), //
                getBlockElement(4, L1,4 ) //
        );
        List<DataBlockElement> lst3 = Arrays.asList( //
                getBlockElement(1, L1,1), //
                getBlockElement(1, L2,2), //
                getBlockElement(2, L2,2 ), //
                getBlockElement(3, L2,2 ), //
                getBlockElement(3, L1,3 ) //
        );
        List<DataBlockElement> lst4 = Arrays.asList( //
                // block 1
                getBlockElement(1, L1,1), //
                getBlockElement(1, L1,2 ), //
                getBlockElement(1, L1,3 ), //
                getBlockElement(1, L1,4 ), //
                // block 2
                getBlockElement(2, L1,1), //
                getBlockElement(3, L1,3 ), //
                // block 3
                getBlockElement(3, L2,2), //
                getBlockElement(3, L2,4 ) //
        );
        return new Object[][] { //
                { lst1, 4 }, //
                { lst2, 3 }, //
                { lst3, 2 }, //
                { lst4, 2 }, //
        };
    }

    private DataBlockElement getBlockElement(int block, DataBlockLevel level, int element) {
        DataBlockElement blockElement = new DataBlockElement();
        PrimeColumn column = new PrimeColumn(String.format("element%d", element), "", "", "");
        blockElement.setPrimeColumn(column);
        blockElement.setBlock(String.format("block%d", block));
        blockElement.setLevel(level);
        blockElement.setFqBlockId(String.format("block%d_%s_v1", block, level));
        return blockElement;
    }

}
