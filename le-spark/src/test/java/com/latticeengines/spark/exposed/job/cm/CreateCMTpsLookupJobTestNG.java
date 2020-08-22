package com.latticeengines.spark.exposed.job.cm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cm.CMTpsLookupCreationConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class CreateCMTpsLookupJobTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        List<Runnable> runnables = new ArrayList<>();
        runnables.add(this::testCreateTpsLookup);
        ThreadPoolUtils.runInParallel(this.getClass().getSimpleName(), runnables);
    }

    private void testCreateTpsLookup() {
        List<String> input = prepareData();
        CMTpsLookupCreationConfig config = new CMTpsLookupCreationConfig();
        config.setKey("SITE_DUNS_NUMBER");
        config.setTargetColumn("RECORD_ID");
        SparkJobResult result = runSparkJob(CreateCMTpsLookupJob.class, config, input,
                String.format("/tmp/%s/%s/testCreateTpsLookup", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyTpsLookup));
    }

    private Boolean verifyTpsLookup(HdfsDataUnit tpsLookup) {
        Iterator<GenericRecord> iter = verifyAndReadTarget(tpsLookup);
        int rows = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iter) {
            int siteDuns = (int) record.get("SITE_DUNS_NUMBER");
            switch (siteDuns) {
            case 1014559:
                // System.out.println(record);
                Assert.assertEquals(record.get("RECORD_IDS").toString(), "10075525003,18813140529");
                break;
            case 1017490:
                System.out.println(record);
                Assert.assertEquals(record.get("RECORD_IDS").toString(), "10076299495");
                break;
            case 1089858:
                // System.out.println(record);
                Assert.assertEquals(record.get("RECORD_IDS").toString(), "10072300773");
                break;
            default:
                Assert.fail("Should not see a record with SITE_DUNS_NUMBER " + siteDuns + ": " + record.toString());
            }

            rows++;
        }

        Assert.assertEquals(rows, 3);
        return true;
    }

    private List<String> prepareData() {
        List<String> input = new ArrayList<>();
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("SITE_DUNS_NUMBER", Integer.class), //
                Pair.of("RECORD_ID", String.class), //
                Pair.of("DNB_JOB_TITLE", String.class), //
                Pair.of("DNB_JOB_FUNCTION", String.class), //
                Pair.of("ULTIMATE_DUNS_NUMBER", String.class), //
                Pair.of("DOMESTIC_ULTIMATE_DUNS_NUMBER", String.class), //
                Pair.of("LIVERAMP_LR_MATCH", String.class), //
                Pair.of("GOOGLE_LR_MATCH", String.class), //
                Pair.of("ADOBE_LR_MATCH", String.class), //
                Pair.of("TRADEDESK_LR_MATCH", String.class), //
                Pair.of("VERIZIONMEDIA_LR_MATCH", String.class), //
                Pair.of("APPNEXUS_LR_MATCH", String.class), //
                Pair.of("MEIDAMATH_LR_MATCH", String.class));
        Object[][] data = getInput1Data();
        input.add(uploadHdfsDataUnit(data, fields));

        return input;
    }

    private Object[][] getInput1Data() {
        Object[][] data = new Object[][] { //
                { 1014559, "10075525003", "senior director of Engineering", "Software Engineering", null, null, "f",
                        "f", "f", "f", "f", "f", "f" }, //
                { 1014559, "18813140529", "VP", "Marketing", null, null, "f", "f", "f", "f", "f", "f", "f" }, //
                { 1017490, "10076299495", "sales representative", "Sales Support", "1017490", "1017490", "t", "t", "t",
                        "t", "t", "t", "f" }, //
                { 1017490, null, "Teacher", "Education", "1017999", "1017999", "t", "t", "t", "t", "t", "t", "f" }, //
                { null, "11082300999", "CEO", "CEO", "1018666", "1018666", "f", "t", "f", "t", "t", "t", "f" }, //
                { 1089858, "10072300773", "contributing editor", "Writer/Editor", "6962435", "6962435", "t", "f", "t",
                        "f", "t", "t", "f" } };
        return data;
    }
}
