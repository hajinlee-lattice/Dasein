package com.latticeengines.pls.util;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.modeling.PivotValuesLookup;
import com.latticeengines.domain.exposed.util.ModelingUtils;

public class PivotMappingFileUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testCreateAttrsFromPivotSourceColumns() throws Exception {
        String path = ClassLoader.getSystemResource("com/latticeengines/pls/util/wrong_pivot_mapping_file.txt")
                .getPath();
        Configuration config = new Configuration();
        config.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        PivotValuesLookup pivotValuesLookup = ModelingUtils.getPivotValues(config, path);

        Attribute attr1 = new Attribute();
        attr1.setName("a");
        attr1.setDisplayName("a");
        attr1.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);

        Attribute attr2 = new Attribute();
        attr2.setName("b");
        attr2.setDisplayName("b");
        attr2.setApprovedUsage(ApprovedUsage.NONE);
        List<Attribute> newAttrs = PivotMappingFileUtils.createAttrsFromPivotSourceColumns(
                pivotValuesLookup.pivotValuesBySourceColumn.keySet(),
                Arrays.<Attribute> asList(new Attribute[] { attr1, attr2 }));
        System.out.println(newAttrs);
        assertEquals(newAttrs.size(), 2);
        assertEquals(newAttrs.get(0).getName(), attr1.getName());
        assertEquals(newAttrs.get(1).getName(), attr2.getName());
        assertEquals(newAttrs.get(1).getApprovedUsage().get(0), ApprovedUsage.MODEL_ALLINSIGHTS.toString());
    }
}
