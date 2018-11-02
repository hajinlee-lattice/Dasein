package com.latticeengines.datacloud.etl.ingestion.service.impl;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.entitymgr.PatchBookEntityMgr;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionProgressEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProgressService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionVersionService;
import com.latticeengines.datacloud.etl.testframework.DataCloudEtlFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.ingestion.PatchBookConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchMode;

public class IngestionPatchBookProviderServiceImplTestNG extends DataCloudEtlFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(IngestionVersionServiceImplTestNG.class);

    private static List<PatchBook> DOMAIN_PATCH_BOOKS; // Test normal mode for
                                                       // domain patch
    private static List<PatchBook> ATTR_PATCH_BOOKS;// Test hotfix mode for
                                                    // attribute patch
    private static final long TIMESTAMP = System.currentTimeMillis();
    private static final String OPERATOR = IngestionPatchBookProviderServiceImplTestNG.class.getSimpleName();
    private static final Integer ALEXA_RANK = 1;
    private static final String DOMAIN = "google.com";
    private static final String COUNTRY = "country";
    private static final String STATE = "state";
    private static final String ZIPCODE = "zip";

    @Inject
    private IngestionProgressService ingestionProgressService;

    @Inject
    private IngestionProgressEntityMgr ingestionProgressEntityMgr;

    @Inject
    private IngestionPatchBookProviderServiceImpl ingestionProviderService;

    @Inject
    private IngestionEntityMgr ingestionEntityMgr;

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Inject
    private IngestionVersionService ingestionVersionService;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        prepareCleanPod(this.getClass().getSimpleName());
        fakePatchBooks();
        MockitoAnnotations.initMocks(this);
        ingestionProviderService.setPatchBookEntityMgr(mockPatchBookEntityMgr());
    }

    @Test(groups = "functional", dataProvider = "Ingestions")
    public void test(Ingestion ingestion) {
        ingestionEntityMgr.save(ingestion);
        ingestion = ingestionEntityMgr.getIngestionByName(ingestion.getIngestionName());
        PatchBookConfiguration patchConfig = (PatchBookConfiguration) ingestion.getProviderConfiguration();
        IngestionProgress progress = ingestionProgressService.createDraftProgress(ingestion, OPERATOR, null,
                HdfsPathBuilder.dateFormat.format(new Date(TIMESTAMP)));
        progress = ingestionProgressService.saveProgress(progress);
        try {
            ingestionProviderService.ingest(progress);
            // Get progress with updated status
            progress = ingestionProgressEntityMgr.findProgress(progress);
        } catch (Exception e) {
            log.error("Job failed for ingestion " + JsonUtils.serialize(ingestion), e);
            Assert.assertFalse(false);
        } finally {
            ingestionEntityMgr.delete(ingestion);
        }
        Assert.assertEquals(progress.getStatus(), ProgressStatus.FINISHED);
        verify(ingestion, patchConfig);
    }

    private void verify(Ingestion ingestion, PatchBookConfiguration patchConfig) {
        String version = ingestionVersionService.findCurrentVersion(ingestion);
        String hdfsPath = hdfsPathBuilder.constructIngestionDir(ingestion.getIngestionName(), version).toString();
        String glob = new Path(hdfsPath, "*.avro").toString();
        switch (patchConfig.getBookType()) {
        case Domain:
            Assert.assertEquals((long) AvroUtils.count(yarnConfiguration, glob), (long) DOMAIN_PATCH_BOOKS.size());
            break;
        case Attribute:
            Assert.assertEquals((long) AvroUtils.count(yarnConfiguration, glob), (long) ATTR_PATCH_BOOKS.size());
            break;
        default:
            break;
        }
        verifyData(hdfsPath, patchConfig.getBookType());
    }

    private void verifyData(String hdfsPath, PatchBook.Type type) {
        Iterator<GenericRecord> records = getGenericRecords(hdfsPath);
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Assert.assertNotNull(record.get(PatchBook.COLUMN_PID));
            long pid = (Long) record.get(PatchBook.COLUMN_PID);
            Assert.assertTrue(isObjEquals(record.get(PatchBook.COLUMN_TYPE), type.name()));
            Assert.assertNotNull(record.getSchema().getField(PatchBook.COLUMN_NAME));
            Assert.assertFalse((Boolean) record.get(PatchBook.COLUMN_EOL));
            Assert.assertTrue(isObjEquals(record.get(PatchBook.COLUMN_CREATEDATE), TIMESTAMP));
            Assert.assertTrue(isObjEquals(record.get(PatchBook.COLUMN_LASTMODIFIEDDATE), TIMESTAMP));
            Assert.assertTrue(isObjEquals(record.get(PatchBook.COLUMN_CREATEBY), OPERATOR));
            Assert.assertTrue(isObjEquals(record.get(PatchBook.COLUMN_LASTMODIFIEDBY), OPERATOR));
            Assert.assertNotNull(record.get(PatchBook.COLUMN_PATCH_ITEMS));
            Map<?, ?> map = JsonUtils.deserialize(record.get(PatchBook.COLUMN_PATCH_ITEMS).toString(), Map.class);
            Map<String, Object> patchItems = JsonUtils.convertMap(map, String.class, Object.class);
            switch (type) {
            case Domain:
                Assert.assertNotNull(record.get(PatchBook.COLUMN_DUNS));
                Assert.assertTrue((Boolean) record.get(PatchBook.COLUMN_CLEANUP));
                Assert.assertFalse((Boolean) record.get(PatchBook.COLUMN_HOTFIX));
                break;
            case Attribute:
                Assert.assertNotNull(record.get(PatchBook.COLUMN_DOMAIN));
                Assert.assertTrue(isObjEquals(record.get(PatchBook.COLUMN_COUNTRY), fakeValue(pid, COUNTRY)));
                Assert.assertTrue(isObjEquals(record.get(PatchBook.COLUMN_STATE), fakeValue(pid, STATE)));
                Assert.assertTrue(isObjEquals(record.get(PatchBook.COLUMN_ZIPCODE), fakeValue(pid, ZIPCODE)));
                Assert.assertEquals(patchItems.get(DataCloudConstants.ATTR_ALEXA_RANK), ALEXA_RANK);
                Assert.assertFalse((Boolean) record.get(PatchBook.COLUMN_CLEANUP));
                Assert.assertTrue((Boolean) record.get(PatchBook.COLUMN_HOTFIX));
                break;
            default:
                break;
            }
        }
    }

    @DataProvider(name = "Ingestions")
    private Object[][] getIngestions() {
        return new Object[][] { //
                { createIngestion(PatchBook.Type.Attribute) }, //
                { createIngestion(PatchBook.Type.Domain) }, //
        };
    }

    private Ingestion createIngestion(PatchBook.Type type) {
        Ingestion ingestion = new Ingestion();
        ingestion.setIngestionName(type + "PatchIngestion");
        ingestion.setIngestionType(Ingestion.IngestionType.PATCH_BOOK);
        PatchBookConfiguration conf = new PatchBookConfiguration();
        conf.setBookType(type);
        conf.setEmailEnabled(false);
        conf.setPatchMode(type == PatchBook.Type.Attribute ? PatchMode.HotFix : PatchMode.Normal);
        ingestion.setConfig(JsonUtils.serialize(conf));
        ingestion.setSchedularEnabled(false);
        ingestion.setNewJobRetryInterval(0L);
        ingestion.setNewJobMaxRetry(0);
        return ingestion;
    }

    private static void fakePatchBooks() {
        DOMAIN_PATCH_BOOKS = new ArrayList<>();
        ATTR_PATCH_BOOKS = new ArrayList<>();
        LongStream.range(0L, 10000L).forEach(i -> {
            PatchBook book = Math.random() > 0.5 ? fakeDomainPatchItem(i) : fakeAttrPatchItem(i);
            book.setEndOfLife(Math.random() > 0.9 ? false : true);
            if (book.getType() == PatchBook.Type.Attribute && !book.isEndOfLife()) {
                book.setHotFix(Math.random() > 0.7 ? true : false);
            } else {
                book.setHotFix(false);
            }
            book.setCreatedBy(OPERATOR);
            book.setLastModifiedBy(OPERATOR);
            book.setCreatedDate(new Date(TIMESTAMP));
            book.setLastModifiedDate(new Date(TIMESTAMP));
            if (book.getType() == PatchBook.Type.Attribute && book.isHotFix()) {
                ATTR_PATCH_BOOKS.add(book);
            }
            if (book.getType() == PatchBook.Type.Domain && !book.isEndOfLife()) {
                DOMAIN_PATCH_BOOKS.add(book);
            }

        });
    }

    // TODO: Change to TestPatchBookUtils
    private static PatchBook fakeDomainPatchItem(long pid) {
        PatchBook book = new PatchBook();
        book.setPid(pid);
        book.setDuns(String.valueOf(pid));
        Map<String, Object> patchItems = new HashMap<>();
        patchItems.put(DataCloudConstants.ATTR_LDC_DOMAIN, "google.com");
        book.setPatchItems(patchItems);
        book.setCleanup(true); // Cleanup mode is only for Domain patch
        book.setType(PatchBook.Type.Domain);
        return book;
    }

    // TODO: Change to TestPatchBookUtils
    private static PatchBook fakeAttrPatchItem(long pid) {
        PatchBook book = new PatchBook();
        book.setPid(pid);
        book.setDomain(fakeValue(pid, DOMAIN));
        book.setCountry(fakeValue(pid, COUNTRY));
        book.setState(fakeValue(pid, STATE));
        book.setZipcode(fakeValue(pid, ZIPCODE));
        Map<String, Object> patchItems = new HashMap<>();
        patchItems.put(DataCloudConstants.ATTR_ALEXA_RANK, ALEXA_RANK);
        book.setPatchItems(patchItems);
        book.setCleanup(false);
        book.setType(PatchBook.Type.Attribute);
        return book;
    }

    private static String fakeValue(long pid, String postfix) {
        return String.valueOf(pid) + "_" + postfix;
    }

    private PatchBookEntityMgr mockPatchBookEntityMgr() {
        PatchBookEntityMgr patchBookEntityMgr = Mockito.mock(PatchBookEntityMgr.class);
        Mockito.when(patchBookEntityMgr.findByTypeAndHotFix(eq(PatchBook.Type.Domain), anyBoolean())) //
                .thenReturn(DOMAIN_PATCH_BOOKS);
        Mockito.when(patchBookEntityMgr.findByTypeAndHotFix(eq(PatchBook.Type.Attribute), anyBoolean())) //
                .thenReturn(ATTR_PATCH_BOOKS);
        return patchBookEntityMgr;
    }
}
