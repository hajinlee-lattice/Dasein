package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.Assert;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils;
import com.latticeengines.datacloud.core.util.PatchBookUtils;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datacloud.match.exposed.service.DunsGuideBookService;
import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBook;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchLog;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchStatus;

public class PatchServiceImplUnitTestNG {
    // Doesn't require DataCloud 2.0.14 dynamo table really exists
    private static final String TEST_DATA_CLOUD_VERSION = "2.0.14";
    // used to fake DUNS (resulting DUNS will be invalid but does not matter in this test)
    private static final String TEST_TARGET_DUNS = "placeholder";
    private static final String TEST_PATCH_DUNS_PREFIX = "PatchDuns";

    private PatchServiceImpl service;

    @BeforeMethod(groups = "unit")
    public void setup() {
        service = new PatchServiceImpl();
    }

    @Test(groups = "unit", dataProvider = "patchAMLookupTable")
    public void testPatchAMLookupTable(AccountLookupUpdateTest[] tests) throws Exception {
        check(tests);

        AccountLookupService lookupService = mockAccountLookupService(tests);
        FieldUtils.writeField(service, "accountLookupService", lookupService, true);

        // mock the update method
        Map<String, AccountLookupEntry> updatedEntries = new HashMap<>();
        Mockito.doAnswer(invocation -> {
            AccountLookupEntry entry = invocation.getArgument(0);
            Assert.assertNotNull(entry);
            Assert.assertNotNull(entry.getId());
            Assert.assertNotNull(entry.getLatticeAccountId());
            // should not update the same entry twice
            Assert.assertFalse(updatedEntries.containsKey(entry.getId()));
            updatedEntries.put(entry.getId(), entry);
            return null;
        }).when(lookupService).updateLookupEntry(Mockito.any(), Mockito.anyString());

        // patch and verify results
        List<PatchBook> books = Arrays.stream(tests).map(test -> test.book).collect(Collectors.toList());
        List<PatchLog> logs = service.patchAMLookupTable(books, TEST_DATA_CLOUD_VERSION, false);
        verifyPatchAMLookup(tests, logs, updatedEntries);
    }

    /*
     * Pair<PatchBook, String> => [ patchBook instance, matchedDUNS ]
     * String[] => array of DUNS that have DunsGuideBook entries
     */
    @Test(groups = "unit", dataProvider = "lookupTargetDunsGuideBooks")
    public void testLookupTargetDunsGuideBooks(
            DunsGuideBookLookupTest[] tests, String[] dunsWithGuideBook) throws Exception {
        // mock internal services and set private fields
        DunsGuideBookService dunsGuideBookService = Mockito.mock(DunsGuideBookService.class);
        RealTimeMatchService realTimeMatchService = Mockito.mock(RealTimeMatchService.class);
        FieldUtils.writeField(service, "dunsGuideBookService", dunsGuideBookService, true);
        FieldUtils.writeField(service, "realTimeMatchService", realTimeMatchService, true);

        // configure mocked DunsGuideBookService and RealTimeMatchService
        List<DunsGuideBook> books = Arrays.stream(dunsWithGuideBook).map(duns -> {
            DunsGuideBook book = new DunsGuideBook();
            book.setId(duns);
            return book;
        }).collect(Collectors.toList());
        List<Pair<MatchKeyTuple, String>> matchResults = Arrays.stream(tests)
                .filter(test -> test.matchedDuns != null)
                .map(test -> Pair.of(PatchBookUtils.getMatchKeyValues(test.book), test.matchedDuns))
                .collect(Collectors.toList());
        mockDunsGuideBookService(dunsGuideBookService, books);
        mockRealTimeMatchService(realTimeMatchService, matchResults);

        // generate expected result
        Map<String, DunsGuideBook> dunsGuideBookMap = books
                .stream()
                .collect(Collectors.toMap(DunsGuideBook::getId, book -> book));
        Map<Long, Pair<String, DunsGuideBook>> expectedTargetGuideBooks = Arrays
                .stream(tests)
                .filter(test -> test.matchedDuns != null)
                .map(test -> Triple.of(
                        test.book.getPid(),
                        test.matchedDuns,
                        test.matchedDuns == null ? null : dunsGuideBookMap.get(test.matchedDuns)))
                .collect(Collectors.toMap(Triple::getLeft, triple -> Pair.of(triple.getMiddle(), triple.getRight())));

        // test with different batch size for real time match and verify the result
        IntStream.range(1, 2 * tests.length).forEach(matchBatchSize -> {
            try {
                lookupDunsGuideBookAndVerify(matchBatchSize, tests, expectedTargetGuideBooks);
            } catch (Exception e) {
                // should not happen
                // forEach does not support checked exception, wrap with unchecked instead
                throw new RuntimeException(e);
            }
        });
    }

    @Test(groups = "unit")
    public void testGetConflictGroupMap() {
        Object[][] testData = prepareConflictGroupTestData();
        Pair<List<PatchBook>, Map<Long, Pair<String, DunsGuideBook>>> params = toGetConflictGroupMapParams(testData);

        Set<Long> booksWithoutConflict = Arrays
                .stream(testData)
                // [ patchBookId, groupIdx ]
                .map(row -> Pair.of((long) row[0], (int) row[6]))
                .filter(pair -> pair.getValue() == -1)
                .map(Pair::getKey)
                .collect(Collectors.toSet());
        Map<Long, Integer> expectedConflictGroupMap = Arrays
                .stream(testData)
                // [ patchBookId, groupIdx ]
                .map(row -> Pair.of((long) row[0], (int) row[6]))
                .filter(pair -> pair.getValue() != -1)
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        Map<Long, Integer> conflictGroupMap = service.getConflictGroupMap(params.getLeft(), params.getRight());
        Assert.assertNotNull(conflictGroupMap);
        verifyConflictGroupMap(conflictGroupMap, expectedConflictGroupMap, booksWithoutConflict);
    }

    @Test(groups = "unit", dataProvider = "updatePatchedDunsGuideBook")
    public void testUpdatePatchedDunsGuideBookEntry(DunsGuideBookUpdateTest[] tests) throws Exception {
        check(tests);
        // prepare input
        List<PatchLog> patchLogs = Arrays.stream(tests).map(test -> {
            PatchLog log = new PatchLog();
            log.setPatchBookId(test.patchBookId);
            log.setInputMatchKey(test.inputMatchKey);
            return log;
        }).collect(Collectors.toList());
        // have a map since the precondition of the method assumes we get the same DunsGuideBook instance for
        // the same matchedDUNS
        Map<String, DunsGuideBook> dunsGuideBookMap = Arrays.stream(tests)
                .filter(test -> test.hasDunsGuideBook)
                .map(test -> {
                    DunsGuideBook book = new DunsGuideBook();
                    book.setId(test.matchedDuns);
                    book.setItems(new ArrayList<>());
                    return book;
                }).collect(Collectors.toMap(DunsGuideBook::getId, book -> book, (v1, v2) -> v1));
        Map<Long, Pair<String, DunsGuideBook>> targetGuideBooks = Arrays
                .stream(tests)
                .map(test -> {
                    if (!test.hasDunsGuideBook) {
                        return Triple.<Long, String, DunsGuideBook>of(test.patchBookId, test.matchedDuns, null);
                    }

                    DunsGuideBook book = dunsGuideBookMap.get(test.matchedDuns);
                    String keyPartition = MatchKeyUtils.evalKeyPartition(test.inputMatchKey);
                    // NOTE should NOT have duplicated key partition in test case
                    Assert.assertNull(getItem(book, keyPartition));
                    if (test.hasKeyPartition) {
                        DunsGuideBook.Item item = new DunsGuideBook.Item();
                        // set to a placeholder so that we know whether the items is updated or not
                        item.setDuns(TEST_TARGET_DUNS);
                        item.setKeyPartition(keyPartition);
                        book.getItems().add(item);
                    }
                    return Triple.of(test.patchBookId, test.matchedDuns, book);
                })
                .collect(Collectors.toMap(
                        Triple::getLeft,
                        triple -> Pair.of(triple.getMiddle(), triple.getRight()),
                        (p1, p2) -> {
                            if (p1.getValue() == null && p2.getValue() == null) {
                                return p1;
                            }

                            // p1.getValue() and p2.getValue() should be both null or both non-null
                            Assert.assertNotNull(p1.getValue());
                            Assert.assertNotNull(p2.getValue());
                            merge(p1.getValue().getItems(), p2.getValue().getItems());
                            return p1;
                        }));
        // use a fake patch DUNS since it does not matter here
        Map<Long, String> patchDunsMap = IntStream.range(0, tests.length)
                .filter(idx -> !tests[idx].cleanup)
                .mapToObj(idx -> Pair.of(tests[idx].patchBookId, TEST_PATCH_DUNS_PREFIX + idx))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        Map<Long, Boolean> cleanupFlagMap = Arrays.stream(tests)
                .map(test -> Pair.of(test.patchBookId, test.cleanup))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        // mock DunsGuideBookService.set method
        DunsGuideBookService dunsGuideBookService = Mockito.mock(DunsGuideBookService.class);
        FieldUtils.writeField(service, "dunsGuideBookService", dunsGuideBookService, true);
        // save all argument to the set method to this map for verification
        Map<String, DunsGuideBook> resultBookMap = new HashMap<>();
        Mockito.doAnswer(invocation -> {
            // might be batched in the future, so not verifying here
            List<DunsGuideBook> books = invocation.getArgument(1);
            for (DunsGuideBook book : books) {
                Assert.assertNotNull(book);
                Assert.assertNotNull(book.getId());
                Assert.assertNotNull(book.getItems());
                // should not have duplicate items
                Assert.assertFalse(resultBookMap.containsKey(book.getId()));
                resultBookMap.put(book.getId(), book);
            }
            return null;
        }).when(dunsGuideBookService).set(Mockito.anyString(), Mockito.anyList());

        // execute and verify
        service.updatePatchedDunsGuideBookEntry(patchLogs, TEST_DATA_CLOUD_VERSION, false,
                targetGuideBooks, patchDunsMap, cleanupFlagMap);
        verifyUpdatePatchedDunsGuideBookEntry(tests, resultBookMap, patchDunsMap);
    }

    private AccountLookupService mockAccountLookupService(@NotNull AccountLookupUpdateTest[] tests) {
        AccountLookupService lookupService = Mockito.mock(AccountLookupService.class);
        Map<String, AccountLookupEntry> entryMap = Arrays
                .stream(tests)
                .flatMap(test -> {
                    MatchKeyTuple tuple = PatchBookUtils.getMatchKeyValues(test.book);
                    AccountLookupEntry target = null;
                    if (test.targetId != null) {
                        target = new AccountLookupEntry();
                        target.setId(getAccountLookupId(tuple));
                        target.setLatticeAccountId(test.targetId);
                    }
                    AccountLookupEntry patch = null;
                    if (test.patchId != null) {
                        patch = new AccountLookupEntry();
                        patch.setId(AccountLookupEntry.buildId(
                                PatchBookUtils.getPatchDomain(test.book), PatchBookUtils.getPatchDuns(test.book)));
                        patch.setLatticeAccountId(test.patchId);
                    }
                    return Stream.of(target, patch);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(AccountLookupEntry::getId, entry -> entry, (v1, v2) -> v1));
        Mockito.when(lookupService.batchLookup(Mockito.any())).thenAnswer(invocation -> {
            AccountLookupRequest req = invocation.getArgument(0);
            return req.getIds().stream().map(entryMap::get).collect(Collectors.toList());
        });
        return lookupService;
    }

    /*
     * Generate AccountLookupEntry ID
     */
    private String getAccountLookupId(@NotNull MatchKeyTuple tuple) {
        if (tuple.getCountry() != null || tuple.getState() != null || tuple.getZipcode() != null) {
            return AccountLookupEntry.buildIdWithLocation(
                    tuple.getDomain(), tuple.getDuns(), tuple.getCountry(), tuple.getState(), tuple.getZipcode());
        } else {
            return AccountLookupEntry.buildId(tuple.getDomain(), tuple.getDuns());
        }
    }

    /*
     * Check if the preconditions of patchAMLookupTable is violated
     */
    private void check(@NotNull AccountLookupUpdateTest[] tests) {
        Arrays.stream(tests).forEach(test -> Assert.assertEquals(test.book.getType(), PatchBook.Type.Lookup));
        List<PatchBook> books = Arrays.stream(tests).map(test -> test.book).collect(Collectors.toList());
        // no duplicates
        Assert.assertTrue(PatchBookUtils.validateDuplicateMatchKey(books).isEmpty());
        // match key should requires patching AccountLookupEntry
        books.forEach(PatchBookUtils::shouldPatchAMLookupTable);
    }

    /*
     * Check if the preconditions of updatePatchedDunsGuideBookEntry is violated
     */
    private void check(@NotNull DunsGuideBookUpdateTest[] tests) {
        Arrays.stream(tests)
                .filter(test -> test.cleanup)
                // have DunsGuideBook and Item so that we can cleanup
                .forEach(test -> Assert.assertTrue(test.hasDunsGuideBook && test.hasKeyPartition));
        Map<String, Integer> duplicateMap = Arrays.stream(tests)
                .collect(Collectors.toMap(test -> {
                    return test.matchedDuns + MatchKeyUtils.evalKeyPartition(test.inputMatchKey);
                }, test -> 1, (v1, v2) -> v1 + v2));
        // should not have duplicate matchDUNS + keyPartition in the test case
        duplicateMap.forEach((key, freq) -> Assert.assertEquals(freq, Integer.valueOf(1)));
    }

    /*
     * Merge all DunsGuideBook.Item of l2 into l1
     */
    private void merge(@NotNull List<DunsGuideBook.Item> l1, @NotNull List<DunsGuideBook.Item> l2) {
        List<DunsGuideBook.Item> list = l2.stream().filter(item -> {
            for (DunsGuideBook.Item target : l1) {
                if (target.getKeyPartition().equals(item.getKeyPartition())) {
                    // already exists in list 1
                    return false;
                }
            }
            return true;
        }).collect(Collectors.toList());
        l1.addAll(list);
    }

    /*
     * Check the patch log and verify all entries are patched correctly (No-op entries are NOT updated and Patched
     * entries are updated with the correct lattice account ID)
     */
    private void verifyPatchAMLookup(
            @NotNull AccountLookupUpdateTest[] tests, @NotNull List<PatchLog> logs,
            @NotNull Map<String, AccountLookupEntry> updatedEntries) {
        List<PatchLog> expectedLogs = Arrays.stream(tests).map(test -> {
            PatchLog log = new PatchLog();
            log.setPatchBookId(test.book.getPid());
            if (test.patchId == null || test.patchId.equals(test.targetId)) {
                // no patch lattice ID or patch lattice ID == target lattice ID
                log.setStatus(PatchStatus.Noop);
            } else {
                log.setStatus(PatchStatus.Patched);
            }
            return log;
        }).collect(Collectors.toList());

        Assert.assertNotNull(logs);
        Assert.assertEquals(logs.size(), expectedLogs.size());
        IntStream.range(0, logs.size()).forEach(idx -> {
            PatchLog log = logs.get(idx);
            PatchLog expectedLog = expectedLogs.get(idx);
            Assert.assertNotNull(log);
            Assert.assertEquals(log.getStatus(), expectedLog.getStatus());
            Assert.assertEquals(log.getPatchBookId(), expectedLog.getPatchBookId());

            String lookupId = getAccountLookupId(PatchBookUtils.getMatchKeyValues(tests[idx].book));
            if (log.getStatus() == PatchStatus.Patched) {
                AccountLookupEntry entry = updatedEntries.get(lookupId);
                Assert.assertNotNull(entry);
                Assert.assertEquals(entry.getLatticeAccountId(), tests[idx].patchId);
            } else {
                // should not update an entry with No-op status
                Assert.assertFalse(updatedEntries.containsKey(lookupId));
            }
        });
    }

    /*
     * Perform dunsGuideBook lookup with specified input and batch size for real time match
     */
    private void lookupDunsGuideBookAndVerify(
            int matchBatchSize, @NotNull DunsGuideBookLookupTest[] tests,
            @NotNull Map<Long, Pair<String, DunsGuideBook>> expectedTargetGuideBooks) throws Exception {
        // set max input size for real time matcher
        FieldUtils.writeField(service, "maxRealTimeInput", matchBatchSize, true);

        // generate input and
        List<PatchBook> patchBooks = Arrays.stream(tests).map(test -> test.book).collect(Collectors.toList());
        Map<Long, Pair<String, DunsGuideBook>> targetGuideBooks = service
                .lookupTargetDunsGuideBooks(patchBooks, TEST_DATA_CLOUD_VERSION);
        Assert.assertNotNull(targetGuideBooks);
        Assert.assertEquals(targetGuideBooks.size(), expectedTargetGuideBooks.size());
        targetGuideBooks.entrySet().forEach(entry -> {
            Assert.assertNotNull(entry);
            Pair<String, DunsGuideBook> result = entry.getValue();
            Pair<String, DunsGuideBook> expectedResult = expectedTargetGuideBooks.get(entry.getKey());
            Assert.assertNotNull(result);
            Assert.assertNotNull(expectedResult);
            // verify matchedDUNS
            Assert.assertEquals(result.getKey(), expectedResult.getKey());
            if (result.getValue() == null) {
                // expected result should have no matched DUNS as well
                Assert.assertNull(expectedResult.getValue());
            } else {
                Assert.assertNotNull(expectedResult.getValue());
                // DunsGuideBook entry has the correct srcDuns
                Assert.assertEquals(result.getValue().getId(), expectedResult.getValue().getId());
            }
        });
    }

    /*
     * Make sure all conflict groups have the same members as the expected groups, and make sure all books without
     * conflict
     */
    private void verifyConflictGroupMap(
            @NotNull Map<Long, Integer> conflictGroupMap, @NotNull Map<Long, Integer> expectedConflictGroupMap,
            @NotNull Set<Long> booksWithoutConflict) {
        Assert.assertEquals(conflictGroupMap.size(), expectedConflictGroupMap.size());

        // conflictGroupIdx -> expectedConflictGroupIdx
        // NOTE don't care what the actual index is, just make sure the same books are in the same group
        Map<Integer, Integer> matchingGroups = new HashMap<>();
        for (Long bookId : conflictGroupMap.keySet()) {
            Integer groupIdx = conflictGroupMap.get(bookId);
            Integer expectedGroupIdx = expectedConflictGroupMap.get(bookId);
            Assert.assertNotNull(groupIdx);
            Assert.assertNotNull(expectedGroupIdx);

            matchingGroups.putIfAbsent(groupIdx, expectedGroupIdx);
            matchingGroups.putIfAbsent(expectedGroupIdx, groupIdx);
            Assert.assertEquals(matchingGroups.get(groupIdx), expectedGroupIdx);
            Assert.assertEquals(matchingGroups.get(expectedGroupIdx), groupIdx);
        }

        // check this just in case, not required
        for (Long bookId : booksWithoutConflict) {
            // should not be in the conflict group map
            Assert.assertFalse(conflictGroupMap.containsKey(bookId));
        }
    }

    private void verifyUpdatePatchedDunsGuideBookEntry(
            DunsGuideBookUpdateTest[] tests, Map<String, DunsGuideBook> resultBookMap, Map<Long, String> patchDunsMap) {
        Arrays.stream(tests).forEach(test -> {
            Assert.assertTrue(resultBookMap.containsKey(test.matchedDuns));

            DunsGuideBook resultBook = resultBookMap.get(test.matchedDuns);
            Assert.assertNotNull(resultBook);
            String keyPartition = MatchKeyUtils.evalKeyPartition(test.inputMatchKey);
            DunsGuideBook.Item resultItem = getItem(resultBook, keyPartition);
            if (test.cleanup) {
                // cleanup successfully
                Assert.assertNull(resultItem);
            } else {
                Assert.assertNotNull(resultItem);
                Assert.assertNotNull(resultItem.getDuns());
                // target DUNS matched
                Assert.assertEquals(resultItem.getDuns(), patchDunsMap.get(test.patchBookId));
                // patch flag set correctly
                Assert.assertTrue(resultItem.getPatched());
            }
        });
    }

    private DunsGuideBook.Item getItem(@NotNull DunsGuideBook book, @NotNull String keyPartition) {
        if (book.getItems() == null) {
            return null;
        }
        for (DunsGuideBook.Item item : book.getItems()) {
            if (item != null && keyPartition.equals(item.getKeyPartition())) {
                return item;
            }
        }
        return null;
    }

    /*
     * Transform to matching types for method signature
     */
    private Pair<List<PatchBook>, Map<Long, Pair<String, DunsGuideBook>>> toGetConflictGroupMapParams(Object[][] data) {
        List<PatchBook> books = Arrays.stream(data).map(row -> {
            Long patchBookId = (long) row[0];
            String name = (String) row[1];
            String country = (String) row[2];
            String state = (String) row[3];
            String patchDuns = (String) row[4];
            PatchBook book = new PatchBook();
            book.setPid(patchBookId);
            book.setName(name);
            book.setCountry(country);
            book.setState(state);
            book.setPatchItems(newPatchItemFromDuns(patchDuns));
            return book;
        }).collect(Collectors.toList());

        Map<Long, Pair<String, DunsGuideBook>> targetGuideBooks = Arrays.stream(data).map(row -> {
            Long patchBookId = (long) row[0];
            String matchedDuns = (String) row[5];
            return Pair.of(patchBookId, Pair.of(matchedDuns, new DunsGuideBook()));
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        return Pair.of(books, targetGuideBooks);
    }

    @DataProvider(name = "patchAMLookupTable")
    private Object[][] preparePatchAMLookupTableTestData() {
        return new Object[][] {
                // NOTE targets should never have duplicates
                // Case #1: target (the one to be patched) AccountLookupEntry does not exist (should create entry)
                {
                    new AccountLookupUpdateTest[] {
                            // patch two different entries to the same lattice account
                            new AccountLookupUpdateTest(
                                    newLookupTestPatchBook(100L, TestPatchBookUtils.GOOGLE_D_1_1, "google.com",
                                            "123456789"), null, "001"),
                            new AccountLookupUpdateTest(
                                    newLookupTestPatchBook(101L, TestPatchBookUtils.GOOGLE_DC_1_1, "google.com",
                                            "123456789"), null, "001"),
                            // patch to anther lattice account
                            new AccountLookupUpdateTest(
                                    newLookupTestPatchBook(102L, TestPatchBookUtils.GOOGLE_DU_1, "google.com",
                                            "111111111"), null, "002"),
                    }
                },
                // Case #2: patch AccountLookupEntry does not exist (no-op)
                {
                        new AccountLookupUpdateTest[] {
                                new AccountLookupUpdateTest(
                                        newLookupTestPatchBook(200L, TestPatchBookUtils.GOOGLE_D_1_1, "google.com",
                                                "123456789"), "001", null),
                                new AccountLookupUpdateTest(
                                        newLookupTestPatchBook(201L, TestPatchBookUtils.GOOGLE_DC_1_1, "google.com",
                                                "123456789"), "002", null),
                        }
                },
                // Case #3: target is the same as patch (no-op)
                {
                        new AccountLookupUpdateTest[] {
                                new AccountLookupUpdateTest(
                                        newLookupTestPatchBook(300L, TestPatchBookUtils.GOOGLE_D_1_1, "google.com",
                                                "111111111"), "001", "001"),
                                new AccountLookupUpdateTest(
                                        newLookupTestPatchBook(301L, TestPatchBookUtils.GOOGLE_DC_1_1, "google.com",
                                                "222222222"), "002", "002"),
                                new AccountLookupUpdateTest(
                                        newLookupTestPatchBook(302L, TestPatchBookUtils.GOOGLE_DU_1, "google.com",
                                                "333333333"), "003", "003"),
                        }
                },
                // Case #4: target is different than patch (patched successfully)
                {
                        new AccountLookupUpdateTest[] {
                                // should be able to patch multiple targets to the same lattice account
                                new AccountLookupUpdateTest(
                                        newLookupTestPatchBook(400L, TestPatchBookUtils.GOOGLE_D_1_1, "google.com",
                                                "111111111"), "999", "001"),
                                new AccountLookupUpdateTest(
                                        newLookupTestPatchBook(401L, TestPatchBookUtils.GOOGLE_DC_1_1, "google.com",
                                                "111111111"), "888", "001"),
                                // different patch lattice account
                                new AccountLookupUpdateTest(
                                        newLookupTestPatchBook(402L, TestPatchBookUtils.GOOGLE_DU_1, "google.com",
                                                "222222222"), "777", "002"),
                        }
                },
                // Casse #5: Mixing Case #1, #2, #3, #4
                {
                        new AccountLookupUpdateTest[] {
                                // Case #1
                                new AccountLookupUpdateTest(
                                        newLookupTestPatchBook(500L, TestPatchBookUtils.GOOGLE_D_1_1, "google.com",
                                                "111111111"), null, "001"),
                                // Case #2
                                new AccountLookupUpdateTest(
                                        newLookupTestPatchBook(501L, TestPatchBookUtils.GOOGLE_DC_1_1, "google.com",
                                                "222222222"), "002", null),
                                // Case #3
                                new AccountLookupUpdateTest(
                                        newLookupTestPatchBook(502L, TestPatchBookUtils.GOOGLE_DU_1, "google.com",
                                                "333333333"), "003", "003"),
                                // Case #4
                                new AccountLookupUpdateTest(
                                        newLookupTestPatchBook(402L, TestPatchBookUtils.FACEBOOK_D_1, "facebook.com",
                                                "444444444"), "777", "004"),
                        }
                },
        };
    }

    @DataProvider(name = "lookupTargetDunsGuideBooks")
    private Object[][] prepareLookupTargetDunsGuideBooksTestData() {
        return new Object[][] {
                // Case #1: No matched DUNS for all entries
                {
                        new DunsGuideBookLookupTest[] {
                                new DunsGuideBookLookupTest(TestPatchBookUtils.newPatchBook(
                                        100L, TestPatchBookUtils.GOOGLE_N_1_1, null), null),
                                new DunsGuideBookLookupTest(TestPatchBookUtils.newPatchBook(
                                        101L, TestPatchBookUtils.GOOGLE_NC_1_1, null), null),
                                new DunsGuideBookLookupTest(TestPatchBookUtils.newPatchBook(
                                        102L, TestPatchBookUtils.GOOGLE_NCS_2, null), null),
                                new DunsGuideBookLookupTest(TestPatchBookUtils.newPatchBook(
                                        103L, TestPatchBookUtils.GOOGLE_NCZ_1, null), null),
                                new DunsGuideBookLookupTest(TestPatchBookUtils.newPatchBook(
                                        104L, TestPatchBookUtils.GOOGLE_NCSCI_1, null), null),
                        },
                        new String[0]
                },
                // Case #2: Get matched DUNS but all of them does not exist in dynamo
                {
                        new DunsGuideBookLookupTest[] {
                                new DunsGuideBookLookupTest(TestPatchBookUtils.newPatchBook(
                                        200L, TestPatchBookUtils.GOOGLE_N_1_1, null), "000000000"),
                                new DunsGuideBookLookupTest(TestPatchBookUtils.newPatchBook(
                                        201L, TestPatchBookUtils.GOOGLE_NC_1_1, null), "111111111"),
                                new DunsGuideBookLookupTest(TestPatchBookUtils.newPatchBook(
                                        202L, TestPatchBookUtils.GOOGLE_NCS_2, null), "222222222"),
                                // these two are matched to the same DUNS (make sure no problem
                                // when there are duplicate DUNS
                                new DunsGuideBookLookupTest(TestPatchBookUtils.newPatchBook(
                                        203L, TestPatchBookUtils.GOOGLE_NCZ_1, null), "333333333"),
                                new DunsGuideBookLookupTest(TestPatchBookUtils.newPatchBook(
                                        204L, TestPatchBookUtils.GOOGLE_NCSCI_1, null), "333333333"),
                        },
                        new String[0]
                },
                // Case #3: Get matched DUNS and all of them exists in dynamo
                {
                        new DunsGuideBookLookupTest[] {
                                new DunsGuideBookLookupTest(TestPatchBookUtils.newPatchBook(
                                        300L, TestPatchBookUtils.GOOGLE_N_1_1, null), "000000000"),
                                new DunsGuideBookLookupTest(TestPatchBookUtils.newPatchBook(
                                        301L, TestPatchBookUtils.GOOGLE_NC_1_1, null), "111111111"),
                                new DunsGuideBookLookupTest(TestPatchBookUtils.newPatchBook(
                                        302L, TestPatchBookUtils.GOOGLE_NCS_2, null), "222222222"),
                                // these two are matched to the same DUNS (make sure no problem
                                // when there are duplicate DUNS
                                new DunsGuideBookLookupTest(TestPatchBookUtils.newPatchBook(
                                        303L, TestPatchBookUtils.GOOGLE_NCZ_1, null), "333333333"),
                                new DunsGuideBookLookupTest(TestPatchBookUtils.newPatchBook(
                                        304L, TestPatchBookUtils.GOOGLE_NCSCI_1, null), "333333333"),
                        },
                        new String[] { "000000000", "111111111", "222222222", "333333333" }
                },
                // Case #4: mixing case #1, #2 and #3
                {
                        new DunsGuideBookLookupTest[] {
                                new DunsGuideBookLookupTest(TestPatchBookUtils.newPatchBook(
                                        400L, TestPatchBookUtils.GOOGLE_N_1_1, null), null),
                                new DunsGuideBookLookupTest(TestPatchBookUtils.newPatchBook(
                                        401L, TestPatchBookUtils.GOOGLE_NC_1_1, null), null),

                                new DunsGuideBookLookupTest(TestPatchBookUtils.newPatchBook(
                                        402L, TestPatchBookUtils.GOOGLE_NCS_1, null), "999999999"),
                                new DunsGuideBookLookupTest(TestPatchBookUtils.newPatchBook(
                                        403L, TestPatchBookUtils.GOOGLE_NCZ_1, null), "888888888"),

                                new DunsGuideBookLookupTest(TestPatchBookUtils.newPatchBook(
                                        404L, TestPatchBookUtils.FACEBOOK_N_1_1, null), "000000000"),
                                new DunsGuideBookLookupTest(TestPatchBookUtils.newPatchBook(
                                        405L, TestPatchBookUtils.FACEBOOK_NC_1, null), "111111111"),
                                new DunsGuideBookLookupTest(TestPatchBookUtils.newPatchBook(
                                        406L, TestPatchBookUtils.FACEBOOK_NCS_1, null), "222222222"),
                                new DunsGuideBookLookupTest(TestPatchBookUtils.newPatchBook(
                                        407L, TestPatchBookUtils.FACEBOOK_NCSCI_1, null), "333333333"),
                        },
                        new String[] { "000000000", "111111111", "222222222", "333333333" }
                },
                // Case #5: empty list (no PatchBook entries that requires patching DunsGuideBook)
                {
                        new DunsGuideBookLookupTest[0], new String[0]
                }
        };
    }

    /*
     * Provide test data for testGetConflictGroupMap()
     *
     * conflictGroupIdx >= 0 means has conflict
     * conflictGroupIdx == -1 means no conflict
     * NOTE: conflictGroupIdx here can be different than the ones from the method, only represent which items have
     * conflict with each others.
     *
     * Schema for each row:
     * [ PatchBookID, Name, Country, State, patchItems.DUNS, matchedDUNS, conflictGroupIdx ]
     */
    private Object[][] prepareConflictGroupTestData() {
        return new Object[][] {
                // Case #1: Isolated matched DUNS
                // no conflict
                { 100L, "Facebook", null, null, "100000000", "200000000", -1 },

                // Case #2: KeyPartition=Name, matchedDUNS=200000001. three different patchItems.DUNS
                // conflict
                { 101L, "Google1", null, null, "100000001", "200000001", 0 },
                { 102L, "Google2", null, null, "100000002", "200000001", 0 },
                { 103L, "Google3", null, null, "100000003", "200000001", 0 },

                // Case #3: KeyPartition=Name,Country, matchedDUNS=200000001, four different patchItems.DUNS
                // conflict, but not the same group as case #2 because KeyPartition is different
                { 104L, "Google1", "USA", null, "100000004", "200000001", 1 },
                { 105L, "Google2", "USA", null, "100000004", "200000001", 1 },
                { 106L, "Google3", "USA", null, "100000005", "200000001", 1 },
                { 107L, "Google4", "USA", null, "100000005", "200000001", 1 },

                // Case #4: KeyPartition=Name,Country,State, matchedDUNS=200000001, two same patchItems.DUNS
                // no conflict, even though matchedDUNS is the same as case #2 & #3, key partition is different.
                // also, both entries want to patch to the same DUNS, so allow it.
                { 108L, "Google1", "USA", "CA", "100000006", "200000001", -1 },
                { 109L, "Google2", "USA", "CA", "100000006", "200000001", -1 },

                // Case #5: KeyPartition=Name,Country,State, matchedDUNS=200000002, four same patchItems.DUNS
                // no conflict, basically the same as case #4, but with different matchedDUNS
                { 110L, "Lattice1", "USA", "CA", "100000007", "200000002", -1 },
                { 111L, "Lattice2", "USA", "CA", "100000007", "200000002", -1 },
                { 112L, "Lattice3", "USA", "CA", "100000007", "200000002", -1 },
                { 113L, "Lattice4", "USA", "CA", "100000007", "200000002", -1 },

                // Case #6: KeyPartition=Name,Country,State, matchedDUNS=200000003, two different patchItems.DUNS
                // conflict, with different matchedDUNS, so different group
                { 114L, "Netflix1", "USA", "CA", "100000008", "200000003", 2 },
                { 115L, "Netflix2", "USA", "CA", "100000009", "200000003", 2 },
        };
    }

    @DataProvider(name = "updatePatchedDunsGuideBook")
    private Object[][] prepareUpdatePatchedDunsGuideBookEntryTestData() {
        return new Object[][] {
                // Case #1: target DunsGuideBook does not exist (insert)
                { newBaseUpdateTests(100L, false, false) },
                // Case #2: target DunsGuideBook exists, no DunsGuideBook.Item with matching KeyPartition
                // (update DunsGuideBook, insert item)
                { newBaseUpdateTests(200L, true, false) },
                // Case #3: target DunsGuideBook exists and has Item with matching KeyPartition
                { newBaseUpdateTests(300L, true, true) },
                // Case #4: cleanup = true (must satisfy hasDunsGuideBook == hasKeyPartition == true)
                {
                    // NOTE currently in this test case, all DunsGuideBook will have empty items after cleanup, improve
                    // if necessary
                    new DunsGuideBookUpdateTest[] {
                            // multiple items
                            newCleanupDunsGuideBookTest(400L, TestPatchBookUtils.GOOGLE_N_1_1, "123456789"),
                            newCleanupDunsGuideBookTest(401L, TestPatchBookUtils.GOOGLE_NC_1_1, "123456789"),
                            // one item
                            newCleanupDunsGuideBookTest(402L, TestPatchBookUtils.GOOGLE_NC_1_1, "000000000"),
                    }
                },
                // Case #5: Mixing Case #1, #2, #3, #4
                {
                    new DunsGuideBookUpdateTest[] {
                            // Case #1
                            new DunsGuideBookUpdateTest(
                                    500L, false, TestPatchBookUtils.GOOGLE_N_1_1,
                                    "123456789", false, false),
                            // Case #2
                            new DunsGuideBookUpdateTest(
                                    501L, false, TestPatchBookUtils.FACEBOOK_NC_1,
                                    "111111111", true, false),
                            // Case #2 + #3 + #4
                            new DunsGuideBookUpdateTest(
                                    502L, false, TestPatchBookUtils.GOOGLE_NC_1_1,
                                    "222222222", true, false),
                            new DunsGuideBookUpdateTest(
                                    503L, false, TestPatchBookUtils.GOOGLE_NCS_1,
                                    "222222222", true, false),
                            new DunsGuideBookUpdateTest(
                                    504L, false, TestPatchBookUtils.GOOGLE_NCSCI_1,
                                    "222222222", true, true),
                            newCleanupDunsGuideBookTest(505L, TestPatchBookUtils.FACEBOOK_NSCI_1, "222222222"),
                            // Case #3 + #4
                            new DunsGuideBookUpdateTest(
                                    506L, false, TestPatchBookUtils.GOOGLE_NSCI_1,
                                    "333333333", true, true),
                            newCleanupDunsGuideBookTest(507L, TestPatchBookUtils.FACEBOOK_N_1_1, "333333333"),
                    }
                },
        };
    }

    private DunsGuideBookUpdateTest newCleanupDunsGuideBookTest(
            long id, @NotNull MatchKeyTuple tuple, @NotNull String matchedDuns) {
        return new DunsGuideBookUpdateTest(id, true, tuple, matchedDuns, true, true);
    }

    private DunsGuideBookUpdateTest[] newBaseUpdateTests(
            long startId, boolean hasDunsGuideBook, boolean hasKeyPartition) {
        return new DunsGuideBookUpdateTest[] {
                // same matched DUNS with different KeyPartition
                new DunsGuideBookUpdateTest(
                        startId, false, TestPatchBookUtils.GOOGLE_N_1_1,
                        "123456789", hasDunsGuideBook, hasKeyPartition),
                new DunsGuideBookUpdateTest(
                        startId + 1, false, TestPatchBookUtils.GOOGLE_NC_1_1,
                        "123456789", hasDunsGuideBook, hasKeyPartition),
                // different matched DUNS
                new DunsGuideBookUpdateTest(
                        startId + 2, false, TestPatchBookUtils.GOOGLE_NCS_1,
                        "888888888", hasDunsGuideBook, hasKeyPartition),
                new DunsGuideBookUpdateTest(
                        startId + 3, false, TestPatchBookUtils.GOOGLE_NCSCI_1,
                        "888888888", hasDunsGuideBook, hasKeyPartition),
                // only one item for this matched DUNS
                new DunsGuideBookUpdateTest(
                        startId + 4, false, TestPatchBookUtils.GOOGLE_NSCI_1,
                        "111111111", hasDunsGuideBook, hasKeyPartition),
        };
    }

    private Map<String, Object> newPatchItemFromDuns(@NotNull String duns) {
        return Collections.singletonMap(MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.DUNS), duns);
    }

    /*
     * Configure DunsGuideBook service to return DunsGuideBook entry if there are an entry in the input list
     * (with the same source DUNS).
     */
    private void mockDunsGuideBookService(@NotNull DunsGuideBookService service, List<DunsGuideBook> books) {
        Map<String, DunsGuideBook> dunsGuideBookMap = books
                .stream().collect(Collectors.toMap(DunsGuideBook::getId, book -> book));
        Mockito.when(service.get(Mockito.anyString(), Mockito.anyList())).thenAnswer(invocation -> {
            List<String> duns = invocation.getArgument(1);
            return duns.stream().map(dunsGuideBookMap::get).collect(Collectors.toList());
        });
    }

    /*
     * Configure real time match service to return
     * (a) matched DUNS when input MatchKeyTuple exists in the input map
     * (b) null otherwise
     */
    private void mockRealTimeMatchService(
            @NotNull RealTimeMatchService service, @NotNull List<Pair<MatchKeyTuple, String>> matchResults) {
        Map<String, String> matchResultMap = matchResults
                .stream()
                .map(pair -> Pair.of(pair.getKey().buildIdForValue(), pair.getValue()))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue, (v1, v2) -> v1));
        Mockito.when(service.match(Mockito.any())).thenAnswer(invocation -> {
            MatchInput input = invocation.getArgument(0);
            List<OutputRecord> records = input.getData().stream().map(list -> {
                // Domain, Name, Country, State, City, Zipcode
                MatchKeyTuple tuple = new MatchKeyTuple.Builder()
                        .withDomain((String) list.get(0))
                        .withName((String) list.get(1))
                        .withCountry((String) list.get(2))
                        .withState((String) list.get(3))
                        .withCity((String) list.get(4))
                        .withZipcode((String) list.get(5))
                        .build();
                String matchedDuns = matchResultMap.get(tuple.buildIdForValue());
                OutputRecord record = new OutputRecord();
                record.setMatchedDuns(matchedDuns);
                return record;
            }).collect(Collectors.toList());
            MatchOutput output = new MatchOutput("UID");
            output.setResult(records);
            return output;
        });
    }

    /*
     * Generate PatchBook instance for AccountLookupTest
     */
    private PatchBook newLookupTestPatchBook(
            long id, @NotNull MatchKeyTuple tuple, @NotNull String patchDomain, @NotNull String patchDuns) {
        PatchBook book =  TestPatchBookUtils.newPatchBook(
                id, PatchBook.Type.Lookup, false, tuple,
                TestPatchBookUtils.newDomainDunsPatchItems(patchDomain, patchDuns));
        // make sure the test case has valid match keys
        Assert.assertTrue(PatchBookUtils.shouldPatchAMLookupTable(book));
        return book;
    }

    /*
     * Test entity for patchAMLookupTable test
     */
    private class AccountLookupUpdateTest {
        PatchBook book;
        String targetId;
        String patchId;

        AccountLookupUpdateTest(PatchBook book, String targetId, String patchId) {
            this.book = book;
            this.targetId = targetId;
            this.patchId = patchId;
        }
    }

    /*
     * Test entity for lookupTargetDunsGuideBooks test
     */
    private class DunsGuideBookLookupTest {
        PatchBook book;
        String matchedDuns;

        DunsGuideBookLookupTest(PatchBook book, String matchedDuns) {
            this.book = book;
            this.matchedDuns = matchedDuns;
        }
    }

    /*
     * Test entity for updatePatchedDunsGuideBookEntry test
     */
    private class DunsGuideBookUpdateTest {
        long patchBookId;
        boolean cleanup;
        MatchKeyTuple inputMatchKey;
        String matchedDuns;
        // if cleanup == true, this field will be ignored (must be true)
        boolean hasDunsGuideBook;
        boolean hasKeyPartition;

        DunsGuideBookUpdateTest(
                long patchBookId, boolean cleanup, MatchKeyTuple inputMatchKey,
                String matchedDuns, boolean hasDunsGuideBook, boolean hasKeyPartition) {
            this.patchBookId = patchBookId;
            this.cleanup = cleanup;
            this.inputMatchKey = inputMatchKey;
            this.matchedDuns = matchedDuns;
            this.hasDunsGuideBook = hasDunsGuideBook;
            this.hasKeyPartition = hasKeyPartition;
        }
    }
}
