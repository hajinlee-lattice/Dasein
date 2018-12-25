package com.latticeengines.eai.functionalframework;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Set;

import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.MetadataConverter;

public interface EaiFunctionalTestNGInterface {

    default Table createFile(File destDir, String fileName) {
        Configuration conf = new Configuration();
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        Table table = MetadataConverter.getTable(conf, destDir.getPath());
        table.setName(fileName);
        return table;
    }

    default void verifyAllDataNotNullWithNumRows(Configuration config, Table table, int expectedNumRows)
            throws Exception {
        List<Extract> extracts = table.getExtracts();
        long numRows = 0;
        for (Extract extract : extracts) {
            List<String> avroFiles = HdfsUtils.getFilesByGlob(config, extract.getPath());

            for (String avroFile : avroFiles) {
                try (FileReader<GenericRecord> reader = AvroUtils.getAvroFileReader(config,
                        new org.apache.hadoop.fs.Path(avroFile))) {
                    long prev = 0;
                    for (; reader.hasNext(); numRows++) {
                        GenericRecord record = reader.next();
                        long current = Long.valueOf(record.get(InterfaceName.InternalId.name()).toString());
                        assertTrue(current > prev);
                        prev = current;
                    }
                }
                validateErrorFile(config, avroFile);
            }
        }
        assertEquals(numRows, expectedNumRows);
    }

    default void validateErrorFile(Configuration config, String avroFile) throws IOException {
        String errorPath = StringUtils.substringBeforeLast(avroFile, "/") + "/error.csv";
        try (CSVParser parser = new CSVParser(new InputStreamReader(HdfsUtils.getInputStream(config, errorPath)),
                LECSVFormat.format)) {
            Set<String> headers = parser.getHeaderMap().keySet();
            assertTrue(headers.contains("Id"));
            assertTrue(headers.contains("LineNumber"));
            assertTrue(headers.contains("ErrorMessage"));

            List<CSVRecord> records = parser.getRecords();
            assertEquals(records.size(), 11);
        }
    }
}
