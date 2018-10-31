package com.latticeengines.app.exposed.download;

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
import org.apache.commons.io.input.BOMInputStream;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.csv.LECSVFormat;

public class HdfsFileHttpDownloaderUnitTestNG {

    private HdfsFileHttpDownloader downloader = new HdfsFileHttpDownloader();;

    private InputStream inputStream;

    private Map<String, String> nameMap;

    @BeforeClass(groups = "unit")
    public void setup() {
        nameMap = new HashMap<>();
        nameMap.put("LE_EMPLOYEE_RANGE", "Employee Range Edited");
        nameMap.put("EmployeeRangeOrdinal", "Employee Range Ordinal Edited");
        inputStream = ClassLoader.getSystemResourceAsStream("download/topPredictor.csv");
    }

    @Test(groups = "unit")
    public void testFixPredictorDisplayName() throws IOException {
        InputStream stream = downloader.fixPredictorDisplayName(inputStream, nameMap);
        try (InputStreamReader reader = new InputStreamReader(new BOMInputStream(stream, false, ByteOrderMark.UTF_8,
                ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE),
                StandardCharsets.UTF_8);) {
            CSVFormat format = LECSVFormat.format;
            int i = 0;
            try (CSVParser parser = new CSVParser(reader, format)) {
                for (CSVRecord record : parser) {
                    i++;
                    String attrName = record.get("Original Column Name");
                    if ("LE_EMPLOYEE_RANGE".equals(attrName)) {
                        record.get("Attribute Name").equals(nameMap.get(attrName));
                    } else if ("EmployeeRangeOrdinal".equals(attrName)) {
                        record.get("Attribute Name").equals(nameMap.get(attrName));
                    }
                }
            }
            Assert.assertEquals(i, 18);
        }
    }

}
