package com.latticeengines.datacloud.etl.ingestion.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.entitymgr.PatchBookEntityMgr;
import com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.PatchBookUtils;
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
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchMode;

public class IngestionPatchBookProviderServiceImplTestNG extends DataCloudEtlFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(IngestionVersionServiceImplTestNG.class);

    // Test normal mode for domain patch
    private static List<PatchBook> MOCK_DOMAIN_BOOKS;
    // Test hotfix mode for attribute patch
    private static List<PatchBook> MOCK_ATTR_BOOKS;
    // To verify whether patch books are updated as expected
    private static Map<Long, PatchBook> PATCH_BOOKS; // PID -> PatchBook

    private static final String OPERATOR = IngestionPatchBookProviderServiceImplTestNG.class.getSimpleName();
    // To test EffectiveSinceVersion/ExpireAfterVersion update
    private static final String DATACLOUD_VERSION = "2.0.15";
    // To determine EOL or not
    private static final Date CURRENT_DATE = new Date();


    // To fake data
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
    private PatchBookEntityMgr patchBookEntityMgr;

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

    @AfterClass(groups = "functional")
    public void destroy() {
        prepareCleanPod(this.getClass().getSimpleName());
    }

    @Test(groups = "functional", dataProvider = "Ingestions")
    public void test(Ingestion ingestion) {
        ingestionEntityMgr.save(ingestion);
        ingestion = ingestionEntityMgr.getIngestionByName(ingestion.getIngestionName());
        PatchBookConfiguration patchConfig = (PatchBookConfiguration) ingestion.getProviderConfiguration();
        IngestionProgress progress = ingestionProgressService.createDraftProgress(ingestion, OPERATOR, null,
                DATACLOUD_VERSION);
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


    @DataProvider(name = "Ingestions", parallel = false)
    private Object[][] getIngestions() {
        return new Object[][] { //
                { createIngestion(PatchBook.Type.Attribute) }, //
                { createIngestion(PatchBook.Type.Domain) }, //
        };
    }

    private void verify(Ingestion ingestion, PatchBookConfiguration patchConfig) {
        String version = ingestionVersionService.findCurrentVersion(ingestion);
        String hdfsPath = hdfsPathBuilder.constructIngestionDir(ingestion.getIngestionName(), version).toString();
        String glob = new Path(hdfsPath, "*.avro").toString();
        switch (patchConfig.getBookType()) {
            case Domain:
            List<PatchBook> activeDomainBooks = MOCK_DOMAIN_BOOKS.stream() //
                        .filter(book -> !PatchBookUtils.isEndOfLife(book, CURRENT_DATE)
                                && book.getPid() >= 0
                                && book.getPid() < MOCK_DOMAIN_BOOKS.size()) //
                    .collect(Collectors.toList());
                Assert.assertEquals((long) AvroUtils.count(yarnConfiguration, glob),
                        (long) activeDomainBooks.size());
            verifyUpdatedPatchBook(activeDomainBooks);
            break;
        case Attribute:
            List<PatchBook> activeAttrBooks = MOCK_ATTR_BOOKS.stream() //
                        .filter(book -> !PatchBookUtils.isEndOfLife(book, CURRENT_DATE)
                                && book.getPid() >= 0
                                && book.getPid() < MOCK_ATTR_BOOKS.size()) //
                    .collect(Collectors.toList());
                Assert.assertEquals((long) AvroUtils.count(yarnConfiguration, glob),
                        (long) activeAttrBooks.size());
            verifyUpdatedPatchBook(activeAttrBooks);
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
            // CreatedDate, CreatedBy, LastModifiedDate, LastModifiedBy are all
            // fixed value
            Assert.assertTrue(isObjEquals(record.get(PatchBook.COLUMN_CREATEDATE), CURRENT_DATE.getTime()));
            Assert.assertTrue(isObjEquals(record.get(PatchBook.COLUMN_LASTMODIFIEDDATE), CURRENT_DATE.getTime()));
            Assert.assertTrue(isObjEquals(record.get(PatchBook.COLUMN_CREATEBY), OPERATOR));
            Assert.assertTrue(isObjEquals(record.get(PatchBook.COLUMN_LASTMODIFIEDBY), OPERATOR));
            Assert.assertNotNull(record.getSchema().getField(PatchBook.COLUMN_EFFECTIVE_SINCE));
            Assert.assertNotNull(record.getSchema().getField(PatchBook.COLUMN_EXPIRE_AFTER));

            Assert.assertNotNull(record.get(DataCloudConstants.ATTR_PATCH_ITEMS));
            Map<?, ?> map = JsonUtils.deserialize(record.get(DataCloudConstants.ATTR_PATCH_ITEMS).toString(),
                    Map.class);
            Map<String, Object> patchItems = JsonUtils.convertMap(map, String.class, Object.class);
            switch (type) {
            case Domain:
                Assert.assertNotNull(record.get(DataCloudConstants.ATTR_PATCH_DUNS));
                // All domain patch items are populated with Cleanup = true and
                // HotFix = false
                Assert.assertTrue((Boolean) record.get(PatchBook.COLUMN_CLEANUP));
                Assert.assertFalse((Boolean) record.get(PatchBook.COLUMN_HOTFIX));
                // Domain, Name, Country, State, ZipCode are not populated,
                // check whether field exists in schema
                Assert.assertNotNull(record.getSchema().getField(DataCloudConstants.ATTR_PATCH_DOMAIN));
                Assert.assertNotNull(record.getSchema().getField(PatchBook.COLUMN_NAME));
                Assert.assertNotNull(record.getSchema().getField(PatchBook.COLUMN_COUNTRY));
                Assert.assertNotNull(record.getSchema().getField(PatchBook.COLUMN_STATE));
                Assert.assertNotNull(record.getSchema().getField(PatchBook.COLUMN_ZIPCODE));
                break;
            case Attribute:
                Assert.assertNotNull(record.get(DataCloudConstants.ATTR_PATCH_DOMAIN));
                Assert.assertTrue(isObjEquals(record.get(PatchBook.COLUMN_COUNTRY), fakeValue(pid, COUNTRY)));
                Assert.assertTrue(isObjEquals(record.get(PatchBook.COLUMN_STATE), fakeValue(pid, STATE)));
                Assert.assertTrue(isObjEquals(record.get(PatchBook.COLUMN_ZIPCODE), fakeValue(pid, ZIPCODE)));
                Assert.assertEquals(patchItems.get(DataCloudConstants.ATTR_ALEXA_RANK), ALEXA_RANK);
                // All attr patch items are populated with Cleanup = false and
                // HotFix = true
                Assert.assertFalse((Boolean) record.get(PatchBook.COLUMN_CLEANUP));
                Assert.assertTrue((Boolean) record.get(PatchBook.COLUMN_HOTFIX));
                // DUNS is not populated, check whether field exists in schema
                Assert.assertNotNull(record.getSchema().getField(DataCloudConstants.ATTR_PATCH_DUNS));
                break;
            default:
                break;
            }
        }
    }

    /**
     * Verified whether EOL, HotFix, EffectiveSinceVersion, ExpireAfterVersion
     * are updated properly
     * 
     * @param books
     */
    private void verifyUpdatedPatchBook(List<PatchBook> books) {
        books.forEach(book -> {
            boolean expectedEOL = PatchBookUtils.isEndOfLife(book, CURRENT_DATE);
            Assert.assertEquals(book.isEndOfLife(), expectedEOL,
                    "EndOfLife flag validation failed for book (pid=" + book.getPid() + "): "
                            + JsonUtils.serialize(book));
            Assert.assertFalse(book.isHotFix(),
                    "HotFix flag validation failed for book (pid=" + book.getPid() + "): " + JsonUtils.serialize(book));
            if (expectedEOL) {
                Assert.assertEquals(book.getExpireAfterVersion(), DATACLOUD_VERSION,
                        "ExpireAfterVersion validation failed for book (pid=" + book.getPid() + "): "
                                + JsonUtils.serialize(book));
            } else {
                Assert.assertEquals(book.getEffectiveSinceVersion(), DATACLOUD_VERSION,
                        "EffectiveSinceVersion validation failed for book (pid=" + book.getPid() + "): "
                                + JsonUtils.serialize(book));
                Assert.assertNull(book.getExpireAfterVersion(),
                        "ExpireAfterVersion validation failed for book (pid=" + book.getPid() + "): "
                                + JsonUtils.serialize(book));
            }
        });
    }

    private Ingestion createIngestion(PatchBook.Type type) {
        Ingestion ingestion = new Ingestion();
        ingestion.setIngestionName(type + "PatchIngestion");
        ingestion.setIngestionType(Ingestion.IngestionType.PATCH_BOOK);
        PatchBookConfiguration conf = new PatchBookConfiguration();
        conf.setBookType(type);
        conf.setBatchSize(5);
        conf.setEmailEnabled(false);
        conf.setSkipValidation(true);
        // Test HotFix mode for Attribute patch type
        conf.setPatchMode(type == PatchBook.Type.Attribute ? PatchMode.HotFix : PatchMode.Normal);
        ingestion.setConfig(JsonUtils.serialize(conf));
        ingestion.setSchedularEnabled(false);
        ingestion.setNewJobRetryInterval(0L);
        ingestion.setNewJobMaxRetry(0);
        return ingestion;
    }

    private static void fakePatchBooks() {
        MOCK_DOMAIN_BOOKS = new ArrayList<>();
        MOCK_ATTR_BOOKS = new ArrayList<>();
        PATCH_BOOKS = new HashMap<>();
        LongStream.range(0L, 10000L).forEach(i -> {
            PatchBook book = Math.random() > 0.5 ? fakeDomainPatchItem(i) : fakeAttrPatchItem(i);
            updateEOL(book);
            updateHotFix(book);
            book.setCreatedBy(OPERATOR);
            book.setLastModifiedBy(OPERATOR);
            book.setCreatedDate(CURRENT_DATE);
            book.setLastModifiedDate(CURRENT_DATE);
            // Test HotFix mode for Attribute patch type
            if (book.getType() == PatchBook.Type.Attribute && book.isHotFix()) {
                MOCK_ATTR_BOOKS.add(book);
                PATCH_BOOKS.put(book.getPid(), book);
            }
            if (book.getType() == PatchBook.Type.Domain) {
                MOCK_DOMAIN_BOOKS.add(book);
                PATCH_BOOKS.put(book.getPid(), book);
            }
        });
    }

    /**
     * Set EffectiveSince and ExpireAfter by expectedEOL But EOL flag is always
     * set as opposite to verify if it could be fixed
     * 
     * @param book
     */
    private static void updateEOL(PatchBook book) {
        boolean expectedEOL = Math.random() > 0.1 ? false : true;
        if (!expectedEOL) { // Valid books, EOL & EffectiveSinceVersion should
                            // be updated, ExpireAfterVersion should be cleared
            book.setEffectiveSince(new DateTime(CURRENT_DATE).minusDays(10).toDate());
            book.setExpireAfter(new DateTime(CURRENT_DATE).plusDays(10).toDate());
            book.setEndOfLife(true);
            book.setExpireAfterVersion(DATACLOUD_VERSION);
        } else { // Invalid books, EOL & ExpireAfterVersion should be updated
            book.setExpireAfter(new DateTime(CURRENT_DATE).minusDays(10).toDate());
            book.setEndOfLife(false);
        }
    }

    /**
     * Test normal mode for domain patch, Test hotfix mode for attribute patch
     * HotFix flag should be cleared after ingestion
     * 
     * @param book
     */
    private static void updateHotFix(PatchBook book) {
        if (book.getType() == PatchBook.Type.Attribute) {
            book.setHotFix(true);
        } else {
            book.setHotFix(false);
        }
    }

    private static PatchBook fakeDomainPatchItem(long pid) {
        MatchKeyTuple tuple = new MatchKeyTuple.Builder().withDuns(String.valueOf(pid)).build();
        Map<String, Object> patchItems = TestPatchBookUtils.newDomainPatchItems("google.com");
        // Cleanup mode is only for Domain patch
        PatchBook book = TestPatchBookUtils.newPatchBook(pid, PatchBook.Type.Domain, true, tuple, patchItems);
        return book;
    }

    private static PatchBook fakeAttrPatchItem(long pid) {
        MatchKeyTuple tuple = new MatchKeyTuple.Builder() //
                .withDomain(fakeValue(pid, DOMAIN)) //
                .withCountry(fakeValue(pid, COUNTRY)) //
                .withState(fakeValue(pid, STATE)) //
                .withZipcode(fakeValue(pid, ZIPCODE)) //
                .build();
        Map<String, Object> patchItems = new HashMap<>();
        patchItems.put(DataCloudConstants.ATTR_ALEXA_RANK, ALEXA_RANK);
        PatchBook book = TestPatchBookUtils.newPatchBook(pid, PatchBook.Type.Attribute, false, tuple, patchItems);
        return book;
    }

    private static String fakeValue(long pid, String postfix) {
        return String.valueOf(pid) + "_" + postfix;
    }

    @SuppressWarnings("unchecked")
    private PatchBookEntityMgr mockPatchBookEntityMgr() {
        PatchBookEntityMgr patchBookEntityMgr = Mockito.mock(PatchBookEntityMgr.class);

        try {
            when(patchBookEntityMgr.findByTypeAndHotFixWithPaginNoSort(Mockito.anyLong(), Mockito.anyLong(), eq(PatchBook.Type.Domain), any(boolean.class)))
                    .thenAnswer(invocation -> {
                        long minPid = invocation.getArgument(0);
                        long maxPid = invocation.getArgument(1);
                                return MOCK_DOMAIN_BOOKS.stream().filter(
                                        book -> book.getPid() >= minPid && book.getPid() < maxPid)
                                        .collect(Collectors.toList());
                    });

            when(patchBookEntityMgr.findByTypeAndHotFixWithPaginNoSort(Mockito.anyLong(), Mockito.anyLong(), eq(PatchBook.Type.Attribute), any(boolean.class)))
                    .thenAnswer(invocation -> {
                        long minPid = invocation.getArgument(0);
                        long maxPid = invocation.getArgument(1);
                        return MOCK_ATTR_BOOKS.stream().filter(
                                        book -> book.getPid() >= minPid && book.getPid() < maxPid)
                                        .collect(Collectors.toList());
                    });

            doAnswer(inv -> {
                mockSetHotFix((List<Long>) inv.getArguments()[0], (boolean) inv.getArguments()[1]);
                return null;
            }).when(patchBookEntityMgr).setHotFix(any(), any(boolean.class));

            doAnswer(inv -> {
                mockSetEndOfLife((List<Long>) inv.getArguments()[0], (boolean) inv.getArguments()[1]);
                return null;
            }).when(patchBookEntityMgr).setEndOfLife(any(), any(boolean.class));

            doAnswer(inv -> {
                mockSetEffectiveSinceVersion((List<Long>) inv.getArguments()[0], (String) inv.getArguments()[1]);
                return null;
            }).when(patchBookEntityMgr).setEffectiveSinceVersion(any(), any());

            doAnswer(inv -> {
                mockSetExpireAfterVersion((List<Long>) inv.getArguments()[0], (String) inv.getArguments()[1]);
                return null;
            }).when(patchBookEntityMgr).setExpireAfterVersion(any(), any());

            doAnswer(inv -> {
                return mockFindCountByTypeAndHotFix((PatchBook.Type) inv.getArguments()[0],
                        (boolean) inv.getArguments()[1]);
            }).when(patchBookEntityMgr).findCountByTypeAndHotFix(any(), any(boolean.class));

            doAnswer(inv -> {
                return mockFindByTypeAndHotFixWithPaginNoSort((int) inv.getArguments()[0],
                        (int) inv.getArguments()[1], (PatchBook.Type) inv.getArguments()[2],
                        (boolean) inv.getArguments()[3]);
            }).when(patchBookEntityMgr).findByTypeAndHotFixWithPaginNoSort(any(int.class),
                    any(int.class), any(), any(boolean.class));
        } catch (Exception e) {
            log.error("Mock patchBookEntityMgr failed", e);
            throw e;
        }

        return patchBookEntityMgr;
    }

    private void mockSetHotFix(List<Long> pids, boolean hotFix) {
        pids.forEach(pid -> {
            PATCH_BOOKS.get(pid).setHotFix(hotFix);
        });
    }

    private void mockSetEndOfLife(List<Long> pids, boolean endOfLife) {
        pids.forEach(pid -> {
            PATCH_BOOKS.get(pid).setEndOfLife(endOfLife);
        });
    }

    private void mockSetEffectiveSinceVersion(List<Long> pids, String version) {
        pids.forEach(pid -> {
            PATCH_BOOKS.get(pid).setEffectiveSinceVersion(version);
        });
    }

    private void mockSetExpireAfterVersion(List<Long> pids, String version) {
        pids.forEach(pid -> {
            PATCH_BOOKS.get(pid).setExpireAfterVersion(version);
        });
    }

    // Test data with Attribute type is all marked as hotfix while that with
    // Domain type is not hotfix. So no need to do any filter by hotfix here
    private long mockFindCountByTypeAndHotFix(@NotNull PatchBook.Type type, boolean hotFix) {
        switch (type) {
        case Attribute:
            return MOCK_ATTR_BOOKS.size();
        case Domain:
            return MOCK_DOMAIN_BOOKS.size();
        default:
            throw new UnsupportedOperationException(
                    "Unsupported PatchBook type in mockFindCountByTypeAndHotFix: " + type);
        }
    }

    // Test data with Attribute type is all mar ked as hotfix while that with
    // Domain type is not hotfix. So no need to do any filter by hotfix here
    private List<PatchBook> mockFindByTypeAndHotFixWithPaginNoSort(int minPid, int maxPid,
            PatchBook.Type type, boolean hotfix) {
        List<PatchBook> books;
        switch (type) {
        case Attribute:
            books = MOCK_ATTR_BOOKS;
            break;
        case Domain:
            books = MOCK_DOMAIN_BOOKS;
            break;
        default:
            throw new UnsupportedOperationException(
                    "Unsupported PatchBook type in mockFindCountByTypeAndHotFix: " + type);
        }
        System.out.println("mock###minPid : " + minPid + " mock##maxPid : " + maxPid);
        List<PatchBook> toReturn = new ArrayList<>();
        for (int i = minPid; i < maxPid; i++) {
            toReturn.add(books.get(i));
        }
        return toReturn;
    }

}
