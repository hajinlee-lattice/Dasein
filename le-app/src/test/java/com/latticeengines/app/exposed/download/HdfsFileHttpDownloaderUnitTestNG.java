package com.latticeengines.app.exposed.download;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.esotericsoftware.minlog.Log;
import com.latticeengines.common.exposed.csv.LECSVFormat;

public class HdfsFileHttpDownloaderUnitTestNG {

    private HdfsFileHttpDownloader downloader = new HdfsFileHttpDownloader();

    private InputStream topPreictorInputStream;

    private InputStream rfModelInputStream;

    private InputStream dateInputStream;

    private Map<String, String> nameMap;

    private Map<String, String> dateMap;

    private final String DATE_FORMAT = "MM/dd/yyyy hh:mm:ss a z";
    private final String[] EXPECTED_DATE_STRINGS = new String[] { "11/06/2018 11:15:15 PM UTC",
            "11/07/2018 04:20:20 AM UTC" };

    @BeforeClass(groups = "unit")
    public void setup() {
        nameMap = new HashMap<>();
        nameMap.put("LE_EMPLOYEE_RANGE", "Employee Range Edited");
        nameMap.put("EmployeeRangeOrdinal", "Employee Range Ordinal Edited");
        nameMap.put("LinkedIn_Url", "LinkedIn Url Edited");

        dateMap = new HashMap<>();
        dateMap.put("CreatedTime", DATE_FORMAT);
        dateMap.put("LastUpdatedTime", DATE_FORMAT);
        topPreictorInputStream = ClassLoader.getSystemResourceAsStream("download/topPredictor.csv");
        rfModelInputStream = ClassLoader.getSystemResourceAsStream("download/rf_model.csv");
        dateInputStream = ClassLoader.getSystemResourceAsStream("download/account.csv");
    }

    @Test(groups = "unit")
    public void testFixPredictorDisplayName() throws IOException {
        StringBuilder sb = new StringBuilder();
        InputStream stream = downloader.fixPredictorDisplayName(topPreictorInputStream, nameMap, sb);
        Assert.assertNotNull(sb);
        Assert.assertTrue(StringUtils.isNotEmpty(sb.toString()));
        Log.info(sb.toString());
        try (InputStreamReader reader = new InputStreamReader(new BOMInputStream(stream, false, ByteOrderMark.UTF_8,
                ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE),
                StandardCharsets.UTF_8)) {
            CSVFormat format = LECSVFormat.format;
            int i = 0;
            try (CSVParser parser = new CSVParser(reader, format)) {
                for (CSVRecord record : parser) {
                    i++;
                    String attrName = record.get("Original Column Name");
                    if ("LE_EMPLOYEE_RANGE".equals(attrName)) {
                        Assert.assertEquals(record.get("Attribute Name"), nameMap.get(attrName));
                    } else if ("EmployeeRangeOrdinal".equals(attrName)) {
                        Assert.assertEquals(record.get("Attribute Name"), nameMap.get(attrName));
                    }
                }
            }
            Assert.assertEquals(i, 18);
        }
    }

    @Test(groups = "unit")
    public void testFixRfModelDisplayName() throws IOException {
        StringBuilder sb = new StringBuilder();
        InputStream stream = downloader.fixRfModelDisplayName(rfModelInputStream, nameMap, sb);
        Assert.assertNotNull(sb);
        Assert.assertTrue(StringUtils.isNotEmpty(sb.toString()));
        Log.info(sb.toString());
        try (InputStreamReader reader = new InputStreamReader(new BOMInputStream(stream, false, ByteOrderMark.UTF_8,
                ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE),
                StandardCharsets.UTF_8)) {
            CSVFormat format = LECSVFormat.format;
            int i = 0;
            try (CSVParser parser = new CSVParser(reader, format)) {
                for (CSVRecord record : parser) {
                    i++;
                    String attrName = record.get("Column Name");
                    if ("LinkedIn_Url".equals(attrName)) {
                        record.get("Column Display Name").equals(nameMap.get(attrName));
                    }
                }
            }
            Assert.assertEquals(i, 55);
        }
    }

    @Test(groups = "unit")
    public void testReformatDates() throws IOException {
        StringBuilder sb = new StringBuilder();
        InputStream stream = downloader.reformatDates(dateInputStream, dateMap, sb);
        try (InputStreamReader reader = new InputStreamReader(new BOMInputStream(stream, false, ByteOrderMark.UTF_8,
                ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE),
                StandardCharsets.UTF_8)) {
            CSVFormat format = LECSVFormat.format;
            int i = 0;
            try (CSVParser parser = new CSVParser(reader, format)) {
                for (CSVRecord record : parser) {
                    i++;
                    String value = record.get("CreatedTime");
                    if (StringUtils.isNotEmpty(value)) {
                        Assert.assertEquals(value, EXPECTED_DATE_STRINGS[0]);
                    }
                    value = record.get("LastUpdatedTime");
                    if (StringUtils.isNotEmpty(value)) {
                        Assert.assertEquals(value, EXPECTED_DATE_STRINGS[1]);
                    }
                }
            }
            Assert.assertEquals(i, 4);

            String filename = sb.toString();
            if (StringUtils.isNotBlank(filename)) {
                System.out.println("Delete temporary file " + filename);
                File file = new File(filename);
                FileUtils.forceDelete(file);
            }
        }
    }

}
