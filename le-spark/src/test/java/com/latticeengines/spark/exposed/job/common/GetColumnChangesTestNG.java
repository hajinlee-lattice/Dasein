package com.latticeengines.spark.exposed.job.common;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.ChangeListConstants;
import com.latticeengines.domain.exposed.spark.common.ColumnChanges;
import com.latticeengines.domain.exposed.spark.common.GetColumnChangesConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class GetColumnChangesTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        uploadData();
        GetColumnChangesConfig config = new GetColumnChangesConfig();
        config.setIncludeAttrs(Arrays.asList("col1", "col2", "col3"));
        SparkJobResult result = runSparkJob(GetColumnChangesJob.class, config);
        verifyResult(result);
    }

    private void uploadData() {
        List<Pair<String, Class<?>>> schema = ChangeListConstants.schema();
        Object[][] data = new Object[][] { //
                // columns with changes
                { "row1", "col1", "String", null, "str1", "str2", null, null, null, null, null, null,
                        null, null, null, null },
                { "row2", "col1", "String", null, "str1", "str2", null, null, null, null, null, null,
                        null, null, null, null },
                { "row3", "col1", "String", null, null, "str2", null, null, null, null, null, null,
                        null, null, null, null },
                { "row4", "col2", "String", true, "str1", null, null, null, null, null, null, null,
                        null, null, null, null },
                { "row5", "col2", "String", true, "str2", null, null, null, null, null, null, null,
                        null, null, null, null },
                // remove columns
                { null, "col3", null, true, null, null, null, null, null, null, null, null,
                        null, null, null, null },
                { null, "col4", null, true, null, null, null, null, null, null, null, null,
                        null, null, null, null },
        };
        uploadHdfsDataUnit(data, schema);
    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Collections.emptyList();
    }

    @Override
    protected void verifyOutput(String output) {
        ColumnChanges changes = JsonUtils.deserialize(output, ColumnChanges.class);
        System.out.println(JsonUtils.pprint(changes));
        Assert.assertEquals(changes.getChanged().size(), 2);
        Assert.assertEquals(changes.getRemoved().size(), 1);
    }

}
