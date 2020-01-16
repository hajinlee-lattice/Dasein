package com.latticeengines.spark.exposed.job.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.zip.GZIPInputStream;

import javax.inject.Inject;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.common.CSVJobConfigBase;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;


public abstract class CSVJobBaseTestNG extends SparkJobFunctionalTestNGBase {

    protected static final String EXPORT_TIME = "ExportTime";
    protected static final long now = System.currentTimeMillis();
    protected static final String FMT_1 = "yyyy.MM.dd HH:mm:ss z";
    protected static final String FMT_2 = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    @Inject
    protected Configuration yarnConfiguration;

    protected static final SimpleDateFormat fmtr1 = new SimpleDateFormat(FMT_1);
    protected static final SimpleDateFormat fmtr2 = new SimpleDateFormat(FMT_2);
    protected static final SimpleDateFormat fmtr3 = new SimpleDateFormat(CSVJobConfigBase.ISO_8601);

    static {
        fmtr1.setTimeZone(TimeZone.getTimeZone("UTC"));
        fmtr2.setTimeZone(TimeZone.getTimeZone("UTC"));
        fmtr3.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        String path = tgt.getPath();
        InputStream is;
        try {
            List<String> files;
            if (this instanceof ConvertToCSVTestNG) {
                files = HdfsUtils.getFilesForDir(yarnConfiguration, path, //
                        (HdfsUtils.HdfsFilenameFilter) filename -> filename.endsWith(".csv.gz"));
                is = new GZIPInputStream(HdfsUtils.getInputStream(yarnConfiguration, files.get(0)));
            } else {
                files = HdfsUtils.getFilesForDir(yarnConfiguration, path, //
                        (HdfsUtils.HdfsFilenameFilter) filename -> filename.endsWith(".csv"));
                is = HdfsUtils.getInputStream(yarnConfiguration, files.get(0));
            }
            Assert.assertEquals(files.size(), 1);
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
            verifyRecord(record);
            count++;
        }
        Assert.assertEquals(tgt.getCount(), Long.valueOf(count));
        return true;
    }

    public abstract void verifyHeaders(Map<String, Integer> headerMap);

    protected void verifyRecord(CSVRecord record) {
        String id = record.get("Id");
        String val1 = record.get("My Attr 1");
        String val2 = record.get("Attr2");
        String val3 = record.get("My Attr 3");
        try {
            switch (id) {
                case "1":
                    verifyRecord1(id, val1, val2, val3);
                    break;
                case "2":
                    verifyRecord2(id, val1, val2, val3);
                    break;
                case "3":
                    verifyRecord3(id, val1, val2, val3);
                    break;
                case "4":
                    verifyRecord4(id, val1, val2, val3);
                    break;
                case "5":
                    verifyRecord5(id, val1, val2, val3);
                    break;
                default:
                    Assert.fail("Should not have this line: " + record);
            }
            if (this instanceof ConvertToCSVTestNG) {
                verifyExportTime(record);
            }
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
