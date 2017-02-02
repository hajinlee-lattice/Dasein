package com.latticeengines.eai.service.impl;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.snowflake.client.jdbc.internal.apache.commons.io.FileUtils;

public class testcsv {

    public static void main(String[] args) throws IOException {
//        try (InputStreamReader reader = new InputStreamReader(new FileInputStream("dropbox_training.csv"))) {
//            CSVFormat format = CSVFormat.RFC4180.withHeader().withQuote(null);
//            int i = 0;
//            try (CSVParser parser = new CSVParser(reader, format)) {
//                for (Iterator<CSVRecord> iterator = parser.iterator(); iterator.hasNext(); i++) {
//                    CSVRecord csvRecord = null;
//                    try {
//                        csvRecord = iterator.next();
//                        if (i == 40371) {
//                            System.out.println(csvRecord);
//                        }
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        }
        Schema s = AvroUtils.readSchemaFromLocalFile("data.avro");
        FileUtils.writeStringToFile(new File("schema.avsc"), s.toString());
    }
}