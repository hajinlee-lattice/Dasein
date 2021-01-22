package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.StringStandardizationUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.entitymgr.DunsGuideBookEntityMgr;
import com.latticeengines.datacloud.match.entitymgr.impl.DunsGuideBookEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBook;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;

@ContextConfiguration(locations = { "classpath:test-datacloud-match-context.xml" })
public class DunsGuideBookServiceImplTestNG extends AbstractTestNGSpringContextTests {

    private static final String TEST_DATACLOUD_VERSION = "2.0.999";
    private static final String TEST_SIGNATURE = "20180829";
    private static final int START_SRC_DUNS = 900000000;
    private static final int START_TARGET_DUNS = 950000000;

    private static final DataCloudVersion version;

    static {
        version = new DataCloudVersion();
        version.setVersion(TEST_DATACLOUD_VERSION);
        version.setDynamoTableSignatureDunsGuideBook(TEST_SIGNATURE);
    }

    @InjectMocks
    @Autowired
    private DunsGuideBookServiceImpl service;

    @Inject
    private FabricDataService dataService;

    @Mock
    private DataCloudVersionEntityMgr mgr;

    private DunsGuideBookEntityMgr dunsGuideBookMgr;

    @BeforeClass(groups = "functional")
    private void setupShared() {
        // mock datacloud version to use the test dynamo table
        MockitoAnnotations.initMocks(this);
        Mockito.when(mgr.findVersion(TEST_DATACLOUD_VERSION)).thenReturn(version);
        // used to setup/cleanup test data
        dunsGuideBookMgr = new DunsGuideBookEntityMgrImpl(dataService,
                TEST_DATACLOUD_VERSION + "_" + TEST_SIGNATURE);
        dunsGuideBookMgr.init();
    }

    @Test(groups = "functional", dataProvider = "dunsGuideBookTestCase")
    public void testGet(TestCase testCase) {
        cleanup(testCase);
        prepare(testCase);
        DunsGuideBook result = service.get(TEST_DATACLOUD_VERSION, testCase.expectedBook.getId());
        verify(result, testCase);
        cleanup(testCase);
    }

    @Test(groups = "functional", dataProvider = "dunsGuideBookListTestCase")
    public void testBatchGet(List<TestCase> testCases) {
        testCases.forEach(this::cleanup);
        testCases.forEach(this::prepare);

        List<String> dunsList = testCases
                .stream()
                .map(testCase -> testCase.expectedBook.getId())
                .collect(Collectors.toList());
        List<DunsGuideBook> results = service.get(TEST_DATACLOUD_VERSION, dunsList);
        Assert.assertNotNull(results);
        Assert.assertEquals(results.size(), testCases.size());
        for (int i = 0; i < testCases.size(); i++) {
            verify(results.get(i), testCases.get(i));
        }
        testCases.forEach(this::cleanup);
    }

    @Test(groups = "functional", dataProvider = "dunsGuideBookListTestCase")
    public void testBatchSetNGet(List<TestCase> testCases) {
        testCases.forEach(this::cleanup);

        // batch create
        List<DunsGuideBook> booksInTable = testCases
                .stream()
                .filter(test -> test.inTable)
                .map(test -> test.expectedBook)
                .collect(Collectors.toList());
        service.set(TEST_DATACLOUD_VERSION, booksInTable);

        List<String> dunsList = testCases
                .stream()
                .map(testCase -> testCase.expectedBook.getId())
                .collect(Collectors.toList());

        try {
            // sleep 1s
            Thread.sleep(TimeUnit.SECONDS.toMillis(1L));
        } catch (InterruptedException e) {
            Assert.fail(e.getMessage());
        }

        List<DunsGuideBook> results = service.get(TEST_DATACLOUD_VERSION, dunsList);
        Assert.assertNotNull(results);
        Assert.assertEquals(results.size(), testCases.size());
        for (int i = 0; i < testCases.size(); i++) {
            verify(results.get(i), testCases.get(i));
        }
        testCases.forEach(this::cleanup);
    }

    @Test(groups = "functional", invocationCount = 200, threadPoolSize = 5)
    public void testConcurrentGetMgr() {
        // simple test just making sure contention will not cause exception
        DunsGuideBookEntityMgr result = service.getEntityMgr(TEST_DATACLOUD_VERSION);
        Assert.assertNotNull(result);
    }

    @DataProvider(name = "dunsGuideBookTestCase")
    private Object[][] provideDunsGuideBookTestCase() {
        return new Object[][] {
                /* valid duns */
                { new TestCase(newTestGuideBook(0, 10), true) },
                { new TestCase(newTestGuideBook(10, 10), false) },
                { new TestCase(newTestGuideBook(123, 1), true) },
                { new TestCase(newTestGuideBook(223, 1), false) },
                { new TestCase(newTestGuideBook(98765, 0), true) },
                { new TestCase(newTestGuideBook(88765, 0), false) },
                /* invalid duns */
                { new TestCase(newTestGuideBook("aabbabc"), true) },
                { new TestCase(newTestGuideBook("aabbabcd"), false) },
                { new TestCase(newTestGuideBook("123456789"), true) },
                { new TestCase(newTestGuideBook("1234567890"), false) },
        };
    }

    @DataProvider(name = "dunsGuideBookListTestCase")
    private Object[][] provideDunsGuideBookListTestCase() {
        return new Object[][] {
                // of means Pair.of
                { newTestCaseList(0, params(Param.TT, Param.TF, Param.FT, Param.FF, Param.TT)) },
                { newTestCaseList(10, params(Param.TT, Param.TT, Param.TT)) },
                { newTestCaseList(20, params(Param.FT, Param.FF, Param.TF, Param.FF, Param.TF)) },
                { newTestCaseList(30, params(Param.FF, Param.FT, Param.TT)) },
                { newTestCaseList(40, params(Param.FF, Param.FF)) },
                { newTestCaseList(50, params(Param.TT)) },
                { newTestCaseList(60, params(Param.TF)) },
                { newTestCaseList(70, params(Param.FT)) },
                { newTestCaseList(80, params(Param.FF)) },
                { newTestCaseList(500, Collections.emptyList()) },
        };
    }

    /*
     * returned list size will be the same as params.size()
     *
     * params[i] = Pair.of(inTable, isDunsValid)
     */
    private List<TestCase> newTestCaseList(int startIdx, List<Pair<Boolean, Boolean>> params) {
        int size = params.size();
        if (size == 0) {
            return Collections.emptyList();
        }

        List<TestCase> cases = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            boolean inTable = params.get(i).getKey();
            boolean isDunsValid = params.get(i).getValue();
            // just use the same values here, does not matter much which invalid duns
            if (!isDunsValid) {
                cases.add(new TestCase(newTestGuideBook("invalid_duns"), inTable));
            } else {
                DunsGuideBook book = newTestGuideBook(startIdx + i, 3);
                cases.add(new TestCase(book, inTable));
            }
        }
        return cases;
    }

    /*
     * helper to generate <inTable, isDunsValid> pair
     */
    private List<Pair<Boolean, Boolean>> params(Param... params) {
        List<Pair<Boolean, Boolean>> list = new ArrayList<>();
        for (Param param : params) {
            if (param == Param.TT) {
                list.add(Pair.of(true, true));
            } else if (param == Param.TF) {
                list.add(Pair.of(true, false));
            } else if (param == Param.FT) {
                list.add(Pair.of(false, true));
            } else {
                list.add(Pair.of(false, false));
            }
        }
        return list;
    }

    private DunsGuideBook newTestGuideBook(String duns) {
        DunsGuideBook book = new DunsGuideBook();
        book.setId(duns);
        book.setItems(Collections.emptyList());
        return book;
    }

    private DunsGuideBook newTestGuideBook(int idx, int nItems) {
        String duns = String.valueOf(START_SRC_DUNS + idx);
        DunsGuideBook book = new DunsGuideBook();
        book.setId(duns);
        book.setItems(new ArrayList<>());
        if (idx % 3 == 1) {
            book.setPatched(true);
        } else if (idx % 3 == 2) {
            book.setPatched(false);
        }
        for (int i = 0; i < nItems; i++) {
            book.getItems().add(newTestItem(idx + i));
        }
        return book;
    }

    private DunsGuideBook.Item newTestItem(int idx) {
        int matchKeySize = MatchKey.values().length;
        String duns = String.valueOf(START_TARGET_DUNS + idx);
        MatchKey key = MatchKey.values()[idx % matchKeySize];
        DunsGuideBook.Item item = new DunsGuideBook.Item();
        item.setDuns(duns);
        item.setKeyPartition(key.name());
        item.setPatched(idx % 2 == 0);
        return item;
    }

    private void prepare(@NotNull TestCase testCase) {
        if (!testCase.inTable) {
            return;
        }

        // populate entry
        dunsGuideBookMgr.create(testCase.expectedBook);
    }

    private void cleanup(@NotNull TestCase testCase) {
        // clear anyways
        dunsGuideBookMgr.delete(testCase.expectedBook);
    }

    private void verify(DunsGuideBook result, @NotNull TestCase testCase) {
        if (!testCase.inTable) {
            Assert.assertNull(result);
            return;
        }

        String standardDuns = StringStandardizationUtils.getStandardDuns(testCase.expectedBook.getId());
        if (standardDuns == null) {
            // invalid duns
            Assert.assertNull(result);
        } else {
            // should be the standardized duns, not the original
            Assert.assertEquals(result.getId(), standardDuns);
            Assert.assertNotNull(result.getItems());
            Assert.assertEquals(result.getItems().size(), testCase.expectedBook.getItems().size());
            Assert.assertEquals(result.isPatched(), testCase.expectedBook.isPatched());
            List<DunsGuideBook.Item> items = result.getItems();
            for (int i = 0; i < items.size(); i++) {
                DunsGuideBook.Item item = items.get(i);
                DunsGuideBook.Item expectedItem = testCase.expectedBook.getItems().get(i);
                Assert.assertNotNull(item);
                Assert.assertEquals(item.getDuns(), expectedItem.getDuns());
                Assert.assertEquals(item.getKeyPartition(), expectedItem.getKeyPartition());
                Assert.assertEquals(item.getPatched(), expectedItem.getPatched());
            }
        }
    }

    /*
     * first character is for TestCase#inTable
     * second caracter is for whether the duns should be valid
     *
     * T => true
     * F => false
     */
    private enum Param {
        TT, TF, FT, FF
    }

    private static class TestCase {
        final DunsGuideBook expectedBook;
        final boolean inTable; // whether the DunsGuideBook exists in dynamo table

        TestCase(@NotNull DunsGuideBook expectedBook, boolean inTable) {
            this.expectedBook = expectedBook;
            this.inTable = inTable;
        }
    }
}
