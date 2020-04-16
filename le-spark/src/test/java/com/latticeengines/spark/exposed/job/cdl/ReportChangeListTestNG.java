package com.latticeengines.spark.exposed.job.cdl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ChangeListConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class ReportChangeListTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ReportChangeListTestNG.class);

    @Test(groups = "functional")
    public void testUpsert() {
        List<Runnable> runnables = new ArrayList<>();
        runnables.add(this::testReportChangeList);
        runnables.add(this::testNewChangeList);
        ThreadPoolUtils.runInParallel(this.getClass().getSimpleName(), runnables);
    }

    private void testReportChangeList() {
        List<String> input = upload1Data();
        ChangeListConfig config = getConfigForChangeList();
        SparkJobResult result = runSparkJob(ReportChangeListJob.class, config, input,
                String.format("/tmp/%s/%s/changeList", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyChangeList));
    }

    private void testNewChangeList() {
        List<String> input = upload2Data();
        ChangeListConfig config = getConfigForChangeList();
        SparkJobResult result = runSparkJob(ReportChangeListJob.class, config, input,
                String.format("/tmp/%s/%s/newChangeList", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyNewChangeList));
    }

    private List<String> upload1Data() {
        List<String> input = new ArrayList<>();
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("RowId", String.class), //
                Pair.of("ColumnId", String.class), //
                Pair.of("DataType", String.class), //
                Pair.of("Deleted", Boolean.class), //
                Pair.of("FromString", String.class), //
                Pair.of("ToString", String.class) //
        );
        Object[][] data = getInput1Data();
        input.add(uploadHdfsDataUnit(data, fields));

        return input;
    }

    private List<String> upload2Data() {
        List<String> input = new ArrayList<>();
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("RowId", String.class), //
                Pair.of("ColumnId", String.class), //
                Pair.of("DataType", String.class), //
                Pair.of("Deleted", Boolean.class), //
                Pair.of("FromString", String.class), //
                Pair.of("ToString", String.class) //
        );
        Object[][] data = getInput2Data();
        input.add(uploadHdfsDataUnit(data, fields));

        return input;
    }

    private Object[][] getInput1Data() {
        Object[][] data = new Object[][] { //
                { 1, "row1", null, "String", false, null, null }, //
                { 2, "row2", null, "String", false, null, null }, //
                { 3, "row3", null, "String", true, null, null }, //
                { 4, "row1", "col1", "String", false, null, "to" }, //
                { 5, "row1", "col2", "String", false, null, "to" }, //
                { 6, "row2", "col1", "String", false, null, "to" }, //
                { 7, "row3", "col1", "String", true, "from3", null }, //
                { 8, "row3", "col3", "String", true, "from3", null }, //
                { 9, "row4", "col1", "String", false, "from4", "to" }, //
                { 10, "row4", "col3", "String", false, "from4", null }, //
        };
        return data;
    }

    private Object[][] getInput2Data() {
        Object[][] data = new Object[][] { //
                { 1, "row1", null, "String", false, null, null }, //
                { 2, "row2", null, "String", false, null, null }, //
                { 4, "row1", "col1", "String", false, null, "to" }, //
                { 5, "row1", "col2", "String", false, null, "to" }, //
                { 6, "row2", "col1", "String", false, null, "to" } //
        };
        return data;
    }

    private ChangeListConfig getConfigForChangeList() {
        ChangeListConfig config = new ChangeListConfig();
        config.setJoinKey("EntityId");
        return config;
    }

    private Boolean verifyChangeList(HdfsDataUnit tgt) {
        Iterator<GenericRecord> iter = verifyAndReadTarget(tgt);
        int rows = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iter) {
            Assert.assertEquals(record.getSchema().getFields().size(), 3, record.toString());
            String newRecords = record.get("NewRecords") != null ? record.get("NewRecords").toString() : null;
            String updatedRecords = record.get("UpdatedRecords") != null ? record.get("UpdatedRecords").toString()
                    : null;
            String deletedRecords = record.get("DeletedRecords") != null ? record.get("DeletedRecords").toString()
                    : null;

            Assert.assertEquals(newRecords, "2", record.toString());
            Assert.assertEquals(updatedRecords, "1", record.toString());
            Assert.assertEquals(deletedRecords, "1", record.toString());
            rows++;
        }
        Assert.assertEquals(rows, 1);
        return true;
    }

    private Boolean verifyNewChangeList(HdfsDataUnit tgt) {
        Iterator<GenericRecord> iter = verifyAndReadTarget(tgt);
        int rows = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iter) {
            Assert.assertEquals(record.getSchema().getFields().size(), 3, record.toString());
            String newRecords = record.get("NewRecords") != null ? record.get("NewRecords").toString() : null;
            String updatedRecords = record.get("UpdatedRecords") != null ? record.get("UpdatedRecords").toString()
                    : null;
            String deletedRecords = record.get("DeletedRecords") != null ? record.get("DeletedRecords").toString()
                    : null;

            Assert.assertEquals(newRecords, "2", record.toString());
            Assert.assertEquals(updatedRecords, "0", record.toString());
            Assert.assertEquals(deletedRecords, "0", record.toString());
            rows++;
        }
        Assert.assertEquals(rows, 1);
        return true;
    }
}
