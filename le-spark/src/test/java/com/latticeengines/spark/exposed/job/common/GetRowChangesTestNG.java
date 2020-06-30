package com.latticeengines.spark.exposed.job.common;

import static com.latticeengines.domain.exposed.spark.common.ChangeListConstants.RowId;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.ChangeListConstants;
import com.latticeengines.domain.exposed.spark.common.GetRowChangesConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class GetRowChangesTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        uploadData();
        GetRowChangesConfig config = new GetRowChangesConfig();
        SparkJobResult result = runSparkJob(GetRowChangesJob.class, config);
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
                // remove rows
                { "row4", null, null, true, null, null, null, null, null, null, null, null,
                        null, null, null, null },
                { "row5", null, null, true, null, null, null, null, null, null, null, null,
                        null, null, null, null },
                // new rows
                { "row1", null, null, null, null, null, null, null, null, null, null, null,
                        null, null, null, null },
                { "row2", null, null, false, null, null, null, null, null, null, null, null,
                        null, null, null, null },
        };
        uploadHdfsDataUnit(data, schema);
    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Arrays.asList(this::verifyNewRows, this::verifyDeletedRows);
    }

    private boolean verifyNewRows(HdfsDataUnit tgt) {
        Set<String> keysToBeFound = new HashSet<>(Arrays.asList("row1", "row2"));
        Set<String> expectedKeys = new HashSet<>(keysToBeFound);
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            String rowId = record.get(RowId).toString();
            Assert.assertTrue(expectedKeys.contains(rowId), "Not expecting row id " + rowId);
            keysToBeFound.remove(rowId);
        });
        Assert.assertTrue(keysToBeFound.isEmpty(), "Not seen row ids " + keysToBeFound);
        return true;
    }

    private boolean verifyDeletedRows(HdfsDataUnit tgt) {
        Set<String> keysToBeFound = new HashSet<>(Arrays.asList("row4", "row5"));
        Set<String> expectedKeys = new HashSet<>(keysToBeFound);
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            String rowId = record.get(RowId).toString();
            Assert.assertTrue(expectedKeys.contains(rowId), "Not expecting row id " + rowId);
            keysToBeFound.remove(rowId);
        });
        Assert.assertTrue(keysToBeFound.isEmpty(), "Not seen row ids " + keysToBeFound);
        return true;
    }

}
