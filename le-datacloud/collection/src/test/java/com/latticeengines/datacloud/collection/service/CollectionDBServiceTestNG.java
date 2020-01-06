package com.latticeengines.datacloud.collection.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.DomainUtils;
import com.latticeengines.datacloud.core.util.S3PathBuilder;
import com.latticeengines.ldc_collectiondb.entity.CollectionWorker;
import com.latticeengines.ldc_collectiondb.entity.VendorConfig;
import com.latticeengines.ldc_collectiondb.entitymgr.CollectionRequestMgr;
import com.latticeengines.ldc_collectiondb.entitymgr.CollectionWorkerMgr;
import com.latticeengines.ldc_collectiondb.entitymgr.VendorConfigMgr;

@DirtiesContext
@ContextConfiguration(locations = {"classpath:test-datacloud-collection-context.xml"})
public class CollectionDBServiceTestNG extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(CollectionDBServiceTestNG.class);

    @Inject
    private CollectionDBService collectionDBService;

    @Inject
    private CollectionWorkerService collectionWorkerService;
    @Value("${datacloud.collection.test.domains}")
    private String testDomains;
    private Timestamp start;
    private Timestamp end;

    @Inject
    S3Service s3Service;

    @Inject
    CollectionWorkerMgr collectionWorkerMgr;

    @Inject
    CollectionRequestMgr collectionRequestMgr;

    @Inject
    VendorConfigMgr vendorConfigMgr;

    @BeforeMethod(groups = "functional")
    public void beforeMethod() {
        start = new Timestamp(System.currentTimeMillis() - 1000);
    }

    @AfterMethod(groups = "functional")
    public void afterMethod() {
        end = new Timestamp(System.currentTimeMillis());
        log.info("start clean up data records ");
        collectionDBService.cleanup(start, end);
    }

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

    @Test(groups = "functional", enabled = false)
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
        boolean finished = false;
        while (!finished)
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
            finished = collectionDBService.collect();
            Thread.sleep(15 * 1000);
        }

        List<CollectionWorker> workers = collectionWorkerService.getWorkerBySpawnTimeBetween(start,
                new Timestamp(System.currentTimeMillis()));
        Assert.assertNotNull(workers);
    }

    @Test(groups = "testCollection", enabled = false)
    public void testCollectionDBService() throws Exception {

        List<String> domains = new ArrayList<>(Arrays.asList(testDomains.split(",")));
        collectionDBService.addNewDomains(domains, UUID.randomUUID().toString().toUpperCase());
        boolean finished = false;
        while (!finished) {
            finished = collectionDBService.collect();
            Thread.sleep(5000);
        }

        collectionDBService.ingest();
        List<CollectionWorker> workers = collectionWorkerService.getWorkerBySpawnTimeBetween(start,
                new Timestamp(System.currentTimeMillis()));
        Assert.assertNotNull(workers);

    }

    @Test(groups = "functional")
    public void testCollection() throws Exception {

        List<String> domains = new ArrayList<>(Arrays.asList(testDomains.split(",")));
        collectionDBService.addNewDomains(domains, UUID.randomUUID().toString().toUpperCase());
        boolean finished = false;
        while (!finished) {
            finished = collectionDBService.collect();
            Thread.sleep(15000);
        }

        List<CollectionWorker> workers = collectionWorkerService.getWorkerBySpawnTimeBetween(start,
                new Timestamp(System.currentTimeMillis()));
        Assert.assertNotNull(workers);
    }

    @Test(groups = "functional")
    public void testVendorConfig() throws Exception {

        List<VendorConfig> orgVendorConfigs = vendorConfigMgr.findAll();
        long maxPid = Long.MIN_VALUE;
        for (VendorConfig vendorConfig: orgVendorConfigs) {
            if (maxPid < vendorConfig.getPid())
                maxPid = vendorConfig.getPid();
        }

        List<VendorConfig> manualCraftedConfigs = new ArrayList<>();
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        for (int i = 0; i < 10; ++i) {
            VendorConfig vendorConfig = new VendorConfig();
            vendorConfig.setPid(0);
            vendorConfig.setVendor(((Integer)i).toString());
            vendorConfig.setGroupBy("domain");
            vendorConfig.setSortBy("CollectedAt");
            vendorConfig.setCollectingFreq(86400L);
            vendorConfig.setConsolidationPeriod(10240);
            vendorConfig.setDomainCheckField("CannotBeAbsent");
            vendorConfig.setDomainField("URL");
            vendorConfig.setMaxActiveTasks(1);
            vendorConfig.setLastConsolidated(ts);

            manualCraftedConfigs.add(vendorConfig);
            vendorConfigMgr.createOrUpdate(vendorConfig);
        }
        List<VendorConfig> vendorConfigsAfterInsertion = vendorConfigMgr.findAll();
        Assert.assertTrue(orgVendorConfigs == null || orgVendorConfigs.size() + 10 == vendorConfigsAfterInsertion.size());

        VendorConfig vendorConfig0 = null;
        for (VendorConfig vendorConfig: vendorConfigsAfterInsertion) {
            if (vendorConfig.getPid() > maxPid) {
                vendorConfig0 = vendorConfig;
                break;
            }
        }
        Assert.assertTrue(vendorConfig0 != null);
        Assert.assertEquals(vendorConfig0.getCollectingFreq(), 86400L);
        Assert.assertEquals(vendorConfig0.getConsolidationPeriod(), 10240);
        Assert.assertEquals(vendorConfig0.getDomainCheckField(), "CannotBeAbsent");
        Assert.assertEquals(vendorConfig0.getDomainField(), "URL");
        Assert.assertEquals(vendorConfig0.getGroupBy(), "domain");
        Assert.assertEquals(vendorConfig0.getMaxActiveTasks(), 1);
        Assert.assertEquals(vendorConfig0.getSortBy(), "CollectedAt");
        Assert.assertEquals(vendorConfig0.getVendor(), "0");

        for (VendorConfig vendorConfig: vendorConfigsAfterInsertion) {
            if (vendorConfig.getPid() > maxPid)
                vendorConfigMgr.delete(vendorConfig);
        }
        Thread.sleep(10000);
        List<VendorConfig> vendorConfigsAfterDel = vendorConfigMgr.findAll();
        Assert.assertTrue(orgVendorConfigs == null || orgVendorConfigs.size() == vendorConfigsAfterDel.size());

        for (VendorConfig vendorConfig: vendorConfigMgr.findAll()) {
            System.out.println(vendorConfig.getVendor());
        }

    }

    @Test(groups = "functional")
    public void testIngestion() throws Exception {

        List<CollectionWorker> consumedWorkers = collectionWorkerMgr.getWorkerByStatus(Arrays.asList(CollectionWorker.STATUS_CONSUMED));
        for (CollectionWorker worker: consumedWorkers) {
            worker.setStatus(CollectionWorker.STATUS_INGESTED);
            collectionWorkerMgr.update(worker);
        }

        String[] orbWorkers = StringUtils.split("05C2CBC5-ADC4-44FD-9408-FA83C350C2E6,271894B8-89EB-4797-A32D-0AA1F0065F49," +
                "286B89BB-F874-40A9-B2C2-575B2D6EE245,3713D933-62FF-49F0-8912-DB6B6532C890", ',');
        String[] alexaWorkers = StringUtils.split("0420DB2A-C938-4C25-BEB9-C2165BF14354,09C3FA50-CE87-452D-BEE5-1209C929D45D," +
                "6B744B27-3AD0-4313-82C2-3F171398E5F5,8B7BB325-E1AE-48B0-B0A0-BF1A7D16B89D,B01DB53F-D787-426E-8F64-56D974E593A3," +
                "C51AD6B8-8CA5-4A0F-B742-0805C3A84E54", ',');

        for (String workerId: orbWorkers) {
            CollectionWorker worker = new CollectionWorker();
            worker.setWorkerId(workerId);
            worker.setStatus(CollectionWorker.STATUS_CONSUMED);
            worker.setRecordsCollected(8192);
            worker.setSpawnTime(new Timestamp(System.currentTimeMillis()));
            worker.setTaskArn("");
            worker.setTerminationTime(new Timestamp(System.currentTimeMillis()));
            worker.setVendor(VendorConfig.VENDOR_ORBI_V2);

            collectionWorkerMgr.createOrUpdate(worker);
        }
        s3Service.cleanupPrefix("latticeengines-dev-datacloud", S3PathBuilder.constructIngestionDir(VendorConfig.VENDOR_ORBI_V2 + "_RAW").toString());

        for (String workerId: alexaWorkers) {
            CollectionWorker worker = new CollectionWorker();
            worker.setWorkerId(workerId);
            worker.setStatus(CollectionWorker.STATUS_CONSUMED);
            worker.setRecordsCollected(8192);
            worker.setSpawnTime(new Timestamp(System.currentTimeMillis()));
            worker.setTaskArn("");
            worker.setTerminationTime(new Timestamp(System.currentTimeMillis()));
            worker.setVendor(VendorConfig.VENDOR_ALEXA);

            collectionWorkerMgr.createOrUpdate(worker);
        }
        s3Service.cleanupPrefix("latticeengines-dev-datacloud", S3PathBuilder.constructIngestionDir(VendorConfig.VENDOR_ALEXA + "_RAW").toString());

        int tasksToIngest = collectionDBService.getIngestionTaskCount();
        Assert.assertEquals(orbWorkers.length + alexaWorkers.length, tasksToIngest);
        collectionDBService.ingest();
        int tasksToIngestAfter = collectionDBService.getIngestionTaskCount();
        Assert.assertEquals(tasksToIngestAfter, 0);

        for (CollectionWorker worker: consumedWorkers) {
            worker.setStatus(CollectionWorker.STATUS_CONSUMED);
            collectionWorkerMgr.update(worker);
        }
    }

    @Test(groups = "testDomainCleanup", enabled = false)
    public void testDomainCleanup() {
        String domain = "verena.solutions";
        log.info(domain);
        log.info(DomainUtils.parseDomain(domain));
    }

    @Test(groups = "functional", enabled = false)
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

    @Test(groups = "functional", enabled = false)
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

        InputStream istream = Thread.currentThread().getContextClassLoader().getResourceAsStream("./part-00000.csv");
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
