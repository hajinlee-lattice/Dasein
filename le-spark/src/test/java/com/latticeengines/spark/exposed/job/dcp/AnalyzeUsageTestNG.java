package com.latticeengines.spark.exposed.job.dcp;

import static com.latticeengines.domain.exposed.datacloud.match.VboUsageConstants.OUTPUT_FIELDS;
import static com.latticeengines.domain.exposed.datacloud.match.VboUsageConstants.RAW_USAGE_DISPLAY_NAMES;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.dcp.AnalyzeUsageConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class AnalyzeUsageTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        uploadData();
        AnalyzeUsageConfig config = new AnalyzeUsageConfig();
        config.setUploadId("Upload_hy6mz3sc");
        config.setDRTAttr("D&B for Sales & Marketing-Domain Use");
        config.setSubscriberCountry("US");
        config.setSubscriberName("D&B Connect Engineering");
        config.setSubscriberNumber("202007222");
        config.setRawOutputMap(RAW_USAGE_DISPLAY_NAMES);
        config.setOutputFields(OUTPUT_FIELDS);
        SparkJobResult result = runSparkJob(AnalyzeUsageJob.class, config);
        verifyResult(result);
    }

    private void uploadData() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("poaeId", String.class), //
                Pair.of("timestamp", String.class), //
                Pair.of("responseTime", Long.class), //
                Pair.of("eventType", String.class), //
                Pair.of("featureUri", String.class), //
                Pair.of("subjectDuns", String.class), //
                Pair.of("subjectName", String.class), //
                Pair.of("subjectCity", String.class), //
                Pair.of("subjectState", String.class), //
                Pair.of("subjectCountry", String.class)
        );
        Object[][] data = getData();
        uploadHdfsDataUnit(data, fields);
    }

    private Object[][] getData() {
        return new Object[][] { //
                {"1", "2020-09-17T02:27:50+00:00", 250, "Match", "clnmat", "1001478", "National Nonwovens Inc.", "Easthampton", "MA", "US"}, //
                {"2", "2020-09-17T02:27:50+00:00", 250, "Match", "clnmat", "1001478", "Banc One Capital Markets, Inc.", "Chicago", "IL", "US"}, //
                {"3", "2020-09-17T02:27:50+00:00", 250, "Match", "clnmat", "1001478", "National Nonwovens Inc.", "Easthampton", "MA", "US"}, //
                {"4", "2020-09-17T02:27:50+00:00", 250, "Match", "clnmat", "1001478", "National Nonwovens Inc.", "Easthampton", "MA", "US"}, //
                {"5", "2020-09-17T02:27:50+00:00", 250, "Data", "companyinfo", "1001478", "National Nonwovens Inc.", "Easthampton", "MA", "US"}, //
                {"6", "2020-09-17T02:27:50+00:00", 250, "Data", "companyinfo", "1001478", "National Nonwovens Inc.", "Easthampton", "MA", "US"}, //
                {"7", "2020-09-17T02:27:50+00:00", 250, "Data", "companyinfo", "1001478", "National Nonwovens Inc.", "Easthampton", "MA", "US"}, //
                {"8", "2020-09-17T02:27:50+00:00", 250, "Data", "companyinfo", "1001478", "National Nonwovens Inc.", "Easthampton", "MA", "US"} //
        };
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        String path = tgt.getPath();
        InputStream is;
        try {
            List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, path, //
                    (HdfsUtils.HdfsFilenameFilter) filename -> filename.endsWith(".csv.gz"));
            Assert.assertEquals(files.size(), 1);
            is = new GZIPInputStream(HdfsUtils.getInputStream(yarnConfiguration, files.get(0)));
        } catch (IOException e) {
            throw new RuntimeException("Failed to read " + path);
        }
        CSVParser records;
        try {
            Reader in = new InputStreamReader(is);
            records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse csv input stream", e);
        }

        verifyHeaders(records.getHeaderMap());

        int count = 0;
        for (CSVRecord record : records) {
            System.out.println(record);
            verifyRecord(record);
            count++;
        }
        Assert.assertEquals(count, getData().length);
        Assert.assertEquals(tgt.getCount(), Long.valueOf(count));
        return true;
    }

    private void verifyHeaders(Map<String, Integer> headerMap) {
        Assert.assertEquals(headerMap.get("GUID"), Integer.valueOf(0));
        Assert.assertEquals(headerMap.get("Client ID (API Key)"), Integer.valueOf(1));
        Assert.assertEquals(headerMap.get("API Key Type"), Integer.valueOf(2));
        Assert.assertEquals(headerMap.get("DRT"), Integer.valueOf(3));
        Assert.assertEquals(headerMap.get("Delivery Mode"), Integer.valueOf(4));
        Assert.assertEquals(headerMap.get("Delivery Channel"), Integer.valueOf(5));
        Assert.assertEquals(headerMap.get("Email Address"), Integer.valueOf(6));
        Assert.assertEquals(headerMap.get("LEID"), Integer.valueOf(7));
        Assert.assertEquals(headerMap.get("LUID"), Integer.valueOf(8));
        Assert.assertEquals(headerMap.get("POAEID"), Integer.valueOf(9));
        Assert.assertEquals(headerMap.get("Subscriber Number"), Integer.valueOf(10));
        Assert.assertEquals(headerMap.get("Subscriber Name"), Integer.valueOf(11));
        Assert.assertEquals(headerMap.get("Agent ID"), Integer.valueOf(12));
        Assert.assertEquals(headerMap.get("APPID"), Integer.valueOf(13));
        Assert.assertEquals(headerMap.get("CAPPID"), Integer.valueOf(14));
        Assert.assertEquals(headerMap.get("Event Type"), Integer.valueOf(15));
        Assert.assertEquals(headerMap.get("TimeStamp"), Integer.valueOf(16));
        Assert.assertEquals(headerMap.get("Feature URI"), Integer.valueOf(17));
        Assert.assertEquals(headerMap.get("Consumer IP"), Integer.valueOf(18));
    }

    private void verifyRecord(CSVRecord record) {
        Assert.assertNotNull(record.get("POAEID"));
        Assert.assertNotNull(record.get("Subscriber Number"));
        Assert.assertNotNull(record.get("DRT"));
        Assert.assertNotNull(record.get("Subject DUNS/Entity ID"));
        Assert.assertNotNull(record.get("Subject City"));
    }
}
