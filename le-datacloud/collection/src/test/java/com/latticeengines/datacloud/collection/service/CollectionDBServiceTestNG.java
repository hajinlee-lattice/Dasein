package com.latticeengines.datacloud.collection.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;

@DirtiesContext
@ContextConfiguration(locations = {"classpath:test-datacloud-collection-context.xml"})
public class CollectionDBServiceTestNG extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(CollectionDBServiceTestNG.class);

    @Inject
    private CollectionDBService collectionDBService;

    @Value("${datacloud.collection.test.domains}")
    private String testDomains;

    private List<String> loadDomains(String path) throws Exception
    {

        CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',')
                .withIgnoreEmptyLines(true).withIgnoreSurroundingSpaces(true);

        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        List<String> ret = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(classloader.getResourceAsStream(path)))) {
            try (CSVParser parser = new CSVParser(reader, format)) {

                Map<String, Integer> colMap = parser.getHeaderMap();
                int domainIdx = colMap.getOrDefault("Domain", -1);

                for (CSVRecord rec : parser) {

                    ret.add(rec.get(domainIdx));

                }

            }

        }

        return ret;

    }

    @Test(groups = "load_test")
    public void testLoad() throws Exception {

        List<String> pathSet = Arrays.asList(
                "50kdomains.0.csv",
                "50kdomains.1.csv",
                "50kdomains.2.csv",
                "50kdomains.3.csv");

        List<List<String>> domainLists = new ArrayList<>();
        for (String path : pathSet) {

            domainLists.add(loadDomains(path));

        }

        long lastTriggered = 0;
        int domainIdx = 0;
        long coolDownPeriod = TimeUnit.MINUTES.toMillis(10); // 10 min
        while (true)
        {

            long curMillis = System.currentTimeMillis();
            if (lastTriggered == 0 || curMillis - lastTriggered > coolDownPeriod) {

                lastTriggered = curMillis;
                log.info("uploading " + domainIdx + "-th domain list.");

                collectionDBService.addNewDomains(domainLists.get(domainIdx), "builtwith", UUID.randomUUID()
                        .toString().toUpperCase());

                log.info("uploaded " + domainLists.get(domainIdx).size() + " domains in " + domainIdx + "-th list.");
                domainIdx = (domainIdx + 1) % domainLists.size();

            }

            collectionDBService.collect();

            Thread.sleep(15 * 1000);

        }

    }

    @Test(groups = "normal_test")
    public void testCollectionDBService() throws Exception {

        List<String> domains = new ArrayList<>(Arrays.asList(testDomains.split(",")));
        collectionDBService.addNewDomains(domains, UUID.randomUUID().toString().toUpperCase());


        while (true)
        {

            collectionDBService.collect();

            Thread.sleep(15000);

        }

    }

    @Test(groups = "ingestion_unit_test")
    public void testIngestionUnit() {

        collectionDBService.ingest();

    }

    @Test(groups = "avro_avsc")
    public void testAvroSchema() throws Exception {
        String schemaStr = AvroUtils.buildSchema("builtwith.avsc");
        Assert.assertNotNull(schemaStr);
        Schema schema = new Schema.Parser().parse(schemaStr);
        Assert.assertNotNull(schema);

        String schemaStr1 = AvroUtils.buildSchema("alexa.avsc");
        Assert.assertNotNull(schemaStr1);
        Schema schema1 = new Schema.Parser().parse(schemaStr1);
        Assert.assertNotNull(schema1);

    }

    @Test(groups = "avro_test")
    public void testAvro() throws Exception {

        String schemaStr = AvroUtils.buildSchema("builtwith.avsc");
        Assert.assertNotNull(schemaStr);
        Schema schema = new Schema.Parser().parse(schemaStr);

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);

        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.setCodec(CodecFactory.deflateCodec(1));
        File tmpFile = File.createTempFile("test", ".avro");
        tmpFile.deleteOnExit();
        log.info("output file: " + tmpFile.getPath());
        dataFileWriter.create(schema, tmpFile);

        List<Schema.Field> fields = schema.getFields();

        InputStream istream = Thread.currentThread().getContextClassLoader().getResourceAsStream("./part-00001.csv");
        //FileInputStream istream = new FileInputStream("./part-00000.csv");
        CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',')
                .withIgnoreEmptyLines(true).withIgnoreSurroundingSpaces(true);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(istream))) {
            try (CSVParser parser = new CSVParser(reader, format)) {

                Map<String, Integer> colMap = parser.getHeaderMap();

                int columns = colMap.size();
                int[] idxMap = new int[columns];
                Schema.Field[] idx2Fields = new Schema.Field[columns];
                if (fields.size() != columns) {

                    throw new Exception("avro column count != csv column count");

                }

                for (Schema.Field field: fields) {

                    int csvIdx = colMap.get(field.name());
                    idxMap[csvIdx] = field.pos();
                    idx2Fields[csvIdx] = field;

                }

                for (CSVRecord csvRec : parser) {

                    GenericRecord rec = new GenericData.Record(schema);
                    for (int i = 0; i < columns; ++i) {

                        rec.put(idxMap[i], AvroUtils.checkTypeAndConvert(idx2Fields[i].name(), //
                                csvRec.get(i), idx2Fields[i].schema().getType()));

                    }

                    try {

                        dataFileWriter.append(rec);

                    } catch (Exception e) {
                        log.error("error csv line: " + csvRec.get("CollectedAt") + ", " + csvRec.get("Spend"));
                    }

                }

            }

        }

        dataFileWriter.close();

    }

}
