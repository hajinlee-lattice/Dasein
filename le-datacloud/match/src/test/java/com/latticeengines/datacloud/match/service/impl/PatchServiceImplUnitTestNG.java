package com.latticeengines.datacloud.match.service.impl;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBook;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PatchServiceImplUnitTestNG {

    private PatchServiceImpl service;

    @BeforeMethod(groups = "unit")
    public void setup() {
        service = new PatchServiceImpl();
    }

    @Test(groups = "unit", enabled = false)
    public void testPatchAMLookupTable() {
        // TODO
    }

    @Test(groups = "unit", enabled = false)
    public void testLookupTargetDunsGuideBooks() {
        // TODO
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

    @Test(groups = "unit", enabled = false)
    public void testUpdatePatchedDunsGuideBookEntry() {
        // TODO
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

                // Case #7: KeyPartition=Name,Country,State, matchedDUNS=200000004, two different patchItems.DUNS
                // conflict, with different matchedDUNS and key partition, so different group
                { 116L, "Amazon1", "USA", null, "100000010", "200000004", 3 },
                { 117L, "Amazon2", "USA", null, "100000011", "200000004", 3 },
        };
    }

    private Map<String, Object> newPatchItemFromDuns(@NotNull String duns) {
        return Collections.singletonMap(MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.DUNS), duns);
    }
}
