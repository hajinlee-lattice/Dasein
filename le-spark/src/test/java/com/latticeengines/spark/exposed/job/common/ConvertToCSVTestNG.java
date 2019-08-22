package com.latticeengines.spark.exposed.job.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.zip.GZIPInputStream;

import javax.inject.Inject;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.ConvertToCSVConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;


public class ConvertToCSVTestNG extends SparkJobFunctionalTestNGBase {

    private static final String EXPORT_TIME = "ExportTime";
    private static final long now = System.currentTimeMillis();
    private static final String FMT_1 = "yyyy.MM.dd HH:mm:ss z";
    private static final String FMT_2 = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    @Inject
    private Configuration yarnConfiguration;

    private static final SimpleDateFormat fmtr1 = new SimpleDateFormat(FMT_1);
    private static final SimpleDateFormat fmtr2 = new SimpleDateFormat(FMT_2);
    private static final SimpleDateFormat fmtr3 = new SimpleDateFormat(ConvertToCSVConfig.ISO_8601);
    static {
        fmtr1.setTimeZone(TimeZone.getTimeZone("UTC"));
        fmtr2.setTimeZone(TimeZone.getTimeZone("UTC"));
        fmtr3.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Test(groups = "functional")
    public void test() {
        uploadData();
        ConvertToCSVConfig config = new ConvertToCSVConfig();
        config.setCompress(true);
        config.setDisplayNames(ImmutableMap.of( //
                "Attr1", "My Attr 1", //
                "Attr3", "My Attr 3" //
        ));
        config.setDateAttrsFmt(ImmutableMap.of( //
                "Attr2", FMT_1, //
                "Attr3", FMT_2, //
                EXPORT_TIME, ConvertToCSVConfig.ISO_8601
        ));
        config.setExportTimeAttr(EXPORT_TIME);
        SparkJobResult result = runSparkJob(ConvertToCSVJob.class, config);
        verifyResult(result);
    }

    private void uploadData() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("Attr1", String.class), //
                Pair.of("Attr2", Long.class), //
                Pair.of("Attr3", Long.class) //
        );
        Object[][] data = new Object[][] { //
                {1, "1", now, now}, //
                {2, "2", null, now}, //
                {3, null, now, now}, //
                {4, "4", now, null}, //
                {5, "Hello world, \"Aloha\", yeah?", now, now}, //
        };
        uploadHdfsDataUnit(data, fields);
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
        Assert.assertEquals(tgt.getCount(), Long.valueOf(count));
        return true;
    }

    private void verifyHeaders(Map<String, Integer> headerMap) {
        Assert.assertEquals(headerMap.get("Id"), Integer.valueOf(0));
        Assert.assertEquals(headerMap.get("My Attr 1"), Integer.valueOf(1));
        Assert.assertEquals(headerMap.get("Attr2"), Integer.valueOf(2));
        Assert.assertEquals(headerMap.get("My Attr 3"), Integer.valueOf(3));
        Assert.assertEquals(headerMap.get(EXPORT_TIME), Integer.valueOf(4));
    }

    private void verifyRecord(CSVRecord record) {
        long rowNum = record.getRecordNumber();
        String id = record.get("Id");
        String val1 = record.get("My Attr 1");
        String val2 = record.get("Attr2");
        String val3 = record.get("My Attr 3");
        try {
            switch ((int) rowNum) {
                case 1:
                    verifyRecord1(id, val1, val2, val3);
                    break;
                case 2:
                    verifyRecord2(id, val1, val2, val3);
                    break;
                case 3:
                    verifyRecord3(id, val1, val2, val3);
                    break;
                case 4:
                    verifyRecord4(id, val1, val2, val3);
                    break;
                case 5:
                    verifyRecord5(id, val1, val2, val3);
                    break;
                default:
                    Assert.fail("Should not have this line: " + record);
            }
            verifyExportTime(record);
        } catch (ParseException e) {
            Assert.fail("Failed to parse date string.", e);
        }
    }

    private void verifyRecord1(String id, String val1, String val2, String val3) throws ParseException {
        Assert.assertEquals(id, "1");
        Assert.assertEquals(val1, "1");
        Assert.assertTrue(withInOneSec(fmtr1.parse(val2).getTime(), now));
        Assert.assertTrue(withInOneSec(fmtr2.parse(val3).getTime(), now));
    }

    private void verifyRecord2(String id, String val1, String val2, String val3) throws ParseException {
        Assert.assertEquals(id, "2");
        Assert.assertEquals(val1, "2");
        Assert.assertEquals(val2, "");
        Assert.assertTrue(withInOneSec(fmtr2.parse(val3).getTime(), now));
    }

    private void verifyRecord3(String id, String val1, String val2, String val3) throws ParseException {
        Assert.assertEquals(id, "3");
        Assert.assertEquals(val1, "");
        Assert.assertTrue(withInOneSec(fmtr1.parse(val2).getTime(), now));
        Assert.assertTrue(withInOneSec(fmtr2.parse(val3).getTime(), now));
    }

    private void verifyRecord4(String id, String val1, String val2, String val3) throws ParseException {
        Assert.assertEquals(id, "4");
        Assert.assertEquals(val1, "4");
        Assert.assertTrue(withInOneSec(fmtr1.parse(val2).getTime(), now));
        Assert.assertEquals(val3, "");
    }

    private void verifyRecord5(String id, String val1, String val2, String val3) throws ParseException {
        Assert.assertEquals(id, "5");
        Assert.assertEquals(val1, "Hello world, \"Aloha\", yeah?");
        Assert.assertTrue(withInOneSec(fmtr1.parse(val2).getTime(), now));
        Assert.assertTrue(withInOneSec(fmtr2.parse(val3).getTime(), now));
    }

    private void verifyExportTime(CSVRecord record) throws ParseException {
        String val = record.get(EXPORT_TIME);
        Assert.assertTrue(betweenThenAndNow(fmtr3.parse(val).getTime(), now));
    }

    private boolean withInOneSec(long ts1, long ts2) {
        return Math.abs(ts1 - ts2) <= 1000;
    }

    private boolean betweenThenAndNow(long ts1, long ts2) {
        return ts1 > ts2 && ts1 < System.currentTimeMillis();
    }

}
