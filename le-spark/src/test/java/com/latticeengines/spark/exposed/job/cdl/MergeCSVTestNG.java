package com.latticeengines.spark.exposed.job.cdl;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.MergeCSVConfig;
import com.latticeengines.domain.exposed.spark.common.ConvertToCSVConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class MergeCSVTestNG extends SparkJobFunctionalTestNGBase {

    private final long now = System.currentTimeMillis();

    private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(ConvertToCSVConfig.ISO_8601);

    @Test(groups = "functional")
    public void test() {
        uploadData();
        MergeCSVConfig config = new MergeCSVConfig();
        SparkJobResult result = runSparkJob(MergeCSVJob.class, config);
        verifyResult(result);
    }

    private void uploadData() {
        String date = simpleDateFormat.format(now);
        String[] headers = Arrays.asList("Id", "Attr1", "Attr2", "Attr3").toArray(new String[]{});
        Object[][] fields = new Object[][]{ //
                {1, "1", now, date}, //
                {2, "2", null, date}, //
                {3, null, now, date}, //
                {4, "4", now, null}, //
                {5, "Hello world, \"Aloha\", yeah?", now, date}, //
        };
        uploadHdfsDataUnitWithCSVFmt(headers, fields);
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        String path = tgt.getPath();
        InputStream is;
        try {
            List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, path, //
                    (HdfsUtils.HdfsFilenameFilter) filename -> filename.endsWith(".csv"));
            is = HdfsUtils.getInputStream(yarnConfiguration, files.get(0));
            Assert.assertEquals(files.size(), 1);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read " + path);
        }
        CSVParser records;
        try {
            Reader in = new InputStreamReader(is);
            records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
            verifyHeaders(records.getHeaderMap());
            Assert.assertEquals(tgt.getCount(), Long.valueOf(records.getRecords().size()));
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse csv input stream", e);
        }
        return true;
    }

    public void verifyHeaders(Map<String, Integer> headerMap) {
        Assert.assertEquals(headerMap.get("Id"), Integer.valueOf(0));
        Assert.assertEquals(headerMap.get("Attr1"), Integer.valueOf(1));
        Assert.assertEquals(headerMap.get("Attr2"), Integer.valueOf(2));
        Assert.assertEquals(headerMap.get("Attr3"), Integer.valueOf(3));
    }

}
