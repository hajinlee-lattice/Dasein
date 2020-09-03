package com.latticeengines.spark.exposed.job.cm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.datacloud.contactmaster.ContactMasterConstants;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cm.CMTpsSourceCreationConfig;
import com.latticeengines.domain.exposed.spark.cm.CMTpsSourceCreationConfig.FieldMapping;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class CreateCMTpsSourceJobTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        List<Runnable> runnables = new ArrayList<>();
        runnables.add(this::testCreateTpsSource);
        ThreadPoolUtils.runInParallel(this.getClass().getSimpleName(), runnables);
    }

    private void testCreateTpsSource() {
        List<String> input = prepareData();
        CMTpsSourceCreationConfig config = new CMTpsSourceCreationConfig();
        List<FieldMapping> fieldMaps = new LinkedList<>();
        FieldMapping map1 = new FieldMapping();
        map1.setNewStandardField(ContactMasterConstants.TPS_STANDARD_JOB_LEVEL);
        map1.setSourceFields(Arrays.asList("DNB_JOB_TITLE", "DNB_JOB_FUNCTION"));
        FieldMapping map2 = new FieldMapping();
        map2.setNewStandardField(ContactMasterConstants.TPS_STANDARD_JOB_FUNCTION);
        map2.setSourceFields(Arrays.asList("DNB_JOB_FUNCTION"));
        fieldMaps.add(map1);
        fieldMaps.add(map2);
        config.setFieldMaps(fieldMaps);
        SparkJobResult result = runSparkJob(CreateCMTpsSourceJob.class, config, input,
                String.format("/tmp/%s/%s/testCreateTpsSource", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyTpsSource));
    }

    private Boolean verifyTpsSource(HdfsDataUnit tpsSource) {
        final AtomicLong count = new AtomicLong();
        verifyAndReadTarget(tpsSource).forEachRemaining(record -> {
            // Generated UUID can't be null
            Assert.assertNotNull(record.get(ContactMasterConstants.TPS_RECORD_UUID));
            int siteDuns = (int) record.get(ContactMasterConstants.TPS_SITE_DUNS_NUMBER);
            switch (siteDuns) {
            case 1014559:
                Assert.assertEquals(record.get(ContactMasterConstants.TPS_STANDARD_JOB_LEVEL).toString(), "Director");
                Assert.assertEquals(record.get(ContactMasterConstants.TPS_STANDARD_JOB_FUNCTION).toString(),
                        "Information Technology");
                break;
            case 1017490:
                Assert.assertEquals(record.get(ContactMasterConstants.TPS_STANDARD_JOB_LEVEL).toString(), "Entry");
                Assert.assertEquals(record.get(ContactMasterConstants.TPS_STANDARD_JOB_FUNCTION).toString(), "Sales");
                break;
            case 1089858:
                Assert.assertEquals(record.get(ContactMasterConstants.TPS_STANDARD_JOB_LEVEL).toString(), "Entry");
                Assert.assertEquals(record.get(ContactMasterConstants.TPS_STANDARD_JOB_FUNCTION).toString(),
                        "Arts and Design");
                break;
            default:
                Assert.fail("Should not see a record with SITE_DUNS_NUMBER " + siteDuns + ": " + record.toString());
            }
            count.addAndGet(1);
        });
        Assert.assertEquals(count.get(), 3L);
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
                { 1017490, "10076299495", "sales representative", "Sales Support", "1017490", "1017490", "t", "t", "t",
                        "t", "t", "t", "f" }, //
                { 1089858, "10072300773", "contributing editor", "Writer/Editor", "6962435", "6962435", "t", "f", "t",
                        "f", "t", "t", "f" } };
        return data;
    }
}
