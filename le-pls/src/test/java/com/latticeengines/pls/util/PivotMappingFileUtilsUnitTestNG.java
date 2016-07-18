package com.latticeengines.pls.util;

import static org.testng.Assert.assertEquals;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;

public class PivotMappingFileUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testCreateAttrsFromPivotSourceColumns() {
        InputStream stream = ClassLoader
                .getSystemResourceAsStream("com/latticeengines/pls/util/wrong_pivot_mapping_file.txt");
        Attribute attr1 = new Attribute();
        attr1.setName("a");
        attr1.setDisplayName("a");
        attr1.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);

        Attribute attr2 = new Attribute();
        attr2.setName("b");
        attr2.setDisplayName("b");
        attr2.setApprovedUsage(ApprovedUsage.NONE);
        List<Attribute> newAttrs = PivotMappingFileUtils.createAttrsFromPivotSourceColumns(stream,
                Arrays.<Attribute> asList(new Attribute[] { attr1, attr2 }));
        System.out.println(newAttrs);
        assertEquals(newAttrs.size(), 2);
        assertEquals(newAttrs.get(0).getName(), attr1.getName());
        assertEquals(newAttrs.get(1).getName(), attr2.getName());
        assertEquals(newAttrs.get(1).getApprovedUsage().get(0), ApprovedUsage.MODEL_ALLINSIGHTS.toString());
    }
}
