package com.latticeengines.datacloud.core.entitymgr.impl;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.inject.Inject;

import org.apache.commons.lang3.time.DateUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.entitymgr.PatchBookEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;

@ContextConfiguration(locations = { "classpath:test-datacloud-core-context.xml" })
public class PatchBookEntityMgrTestNG extends AbstractTestNGSpringContextTests {

    private static final int NUM_PATCH_BOOKS = 10;
    private static final PatchBook TEST_PATCH_BOOK = new PatchBook();

    static {
        // init test patch book
        TEST_PATCH_BOOK.setType(PatchBook.Type.Lookup);
        TEST_PATCH_BOOK.setDomain("google.com");
        TEST_PATCH_BOOK.setDuns("012345678");
        TEST_PATCH_BOOK.setName("Google");
        TEST_PATCH_BOOK.setCleanup(true);
        TEST_PATCH_BOOK.setHotFix(false);
        TEST_PATCH_BOOK.setEndOfLife(true);
        TEST_PATCH_BOOK.setCreatedBy("bot");
        TEST_PATCH_BOOK.setCreatedDate(
                new GregorianCalendar(2018, Calendar.OCTOBER, 10).getTime());
        // Doesn't require DataCloud 2.0.14 dynamo table really exists
        TEST_PATCH_BOOK.setEffectiveSinceVersion("2.0.14");
    }

    @Inject
    private PatchBookEntityMgr patchBookEntityMgr;

    @AfterMethod(groups = "functional")
    public void cleanup() {
        // just in case
        TEST_PATCH_BOOK.setPid(null);
    }

    @Test(groups = "functional", dataProvider = "patchBookQuery")
    private void testQuery(@NotNull PatchBook.Type type, Boolean hotFix) throws Exception {
        // prepare state
        PatchBook book = new PatchBook();
        book.setCleanup(false);
        if (hotFix != null) {
            book.setHotFix(hotFix);
        }
        book.setType(type);
        patchBookEntityMgr.create(book);

        Thread.sleep(1000L);

        Assert.assertNotNull(book.getPid());
        List<PatchBook> books;
        if (hotFix != null) {
            books = patchBookEntityMgr.findByTypeAndHotFix(type, hotFix);
            Assert.assertNotNull(books);

        } else {
            books = patchBookEntityMgr.findByType(type);
        }
        Assert.assertFalse(books.isEmpty()); // should at least have the book we created
        books.forEach(b -> {
            Assert.assertNotNull(b);
            // make sure type & flag matches
            Assert.assertEquals(b.getType(), type);
            if (hotFix != null) {
                Assert.assertEquals(b.isHotFix(), hotFix.booleanValue());
            }
        });

        // cleanup
        patchBookEntityMgr.delete(book);
    }

    /*
     * basic functionality check for entity manager
     */
    @Test(groups = "functional")
    public void testBasicCRUD() {
        /* create */

        patchBookEntityMgr.create(TEST_PATCH_BOOK);
        // PID created
        Assert.assertNotNull(TEST_PATCH_BOOK.getPid());

        /* read */

        PatchBook lookup = new PatchBook();
        lookup.setPid(TEST_PATCH_BOOK.getPid());
        PatchBook result = patchBookEntityMgr.findByKey(lookup);
        Assert.assertNotNull(result);
        assertEquals(result, TEST_PATCH_BOOK);

        /* update and read */

        // set fields
        result.setType(PatchBook.Type.Attribute);
        result.setDuns("999999999");
        result.setLastModifiedBy("functional_test");
        result.setLastModifiedDate(DateUtils.addDays(result.getCreatedDate(), 5));
        // reverse flags
        result.setCleanup(!result.isCleanup());
        result.setHotFix(!result.isHotFix());
        // clear fields
        result.setName(null);
        result.setDomain("xyz.com");
        result.setCreatedDate(null);
        patchBookEntityMgr.update(result);

        PatchBook updatedResult = patchBookEntityMgr.findByKey(lookup);
        Assert.assertNotNull(updatedResult);
        assertEquals(updatedResult, result);

        /* delete */

        patchBookEntityMgr.delete(lookup);
        updatedResult = patchBookEntityMgr.findByKey(lookup);
        assertEquals(updatedResult, null);
    }

    /*
     * test update field for a list of patch books
     */
    @Test(groups = "functional")
    public void testUpdateFieldForMultiplePatchBooks() {
        PatchBook testBook = clone(TEST_PATCH_BOOK);

        /* create test patch books */
        List<Long> pIds = IntStream.range(0, NUM_PATCH_BOOKS).mapToObj(idx -> {
            // clear PID
            testBook.setPid(null);
            patchBookEntityMgr.create(testBook);
            Long pId = testBook.getPid();
            testBook.setPid(null);
            return pId;
        }).collect(Collectors.toList());
        List<PatchBook> lookupBooks = pIds.stream().map(pId -> {
            PatchBook book = new PatchBook();
            book.setPid(pId);
            return book;
        }).collect(Collectors.toList());

        // verify patch books are created successfully
        lookupBooks.forEach(book -> {
            PatchBook result = patchBookEntityMgr.findByKey(book);
            Assert.assertNotNull(result);
            Assert.assertTrue(pIds.contains(result.getPid()));
            // clear PID and verify other fields
            result.setPid(null);
            assertEquals(result, testBook);
        });

        /* update field for all patch books at once and verify each book one by one */

        testBook.setEndOfLife(!testBook.isEndOfLife());
        testBook.setHotFix(!testBook.isHotFix());
        testBook.setEffectiveSinceVersion(null);
        // Doesn't require DataCloud 2.0.14 dynamo table really exists
        testBook.setExpireAfterVersion("2.0.14");
        patchBookEntityMgr.setEndOfLife(pIds, testBook.isEndOfLife());
        patchBookEntityMgr.setHotFix(pIds, testBook.isHotFix());
        patchBookEntityMgr.setEffectiveSinceVersion(pIds, testBook.getEffectiveSinceVersion());
        patchBookEntityMgr.setExpireAfterVersion(pIds, testBook.getExpireAfterVersion());

        lookupBooks.forEach(book -> {
            PatchBook result = patchBookEntityMgr.findByKey(book);
            Assert.assertNotNull(result);
            Assert.assertTrue(pIds.contains(result.getPid()));

            Assert.assertEquals(result.isHotFix(), testBook.isHotFix());
            Assert.assertEquals(result.isEndOfLife(), testBook.isEndOfLife());
            Assert.assertEquals(result.getEffectiveSinceVersion(), testBook.getEffectiveSinceVersion());
            Assert.assertEquals(result.getExpireAfterVersion(), testBook.getExpireAfterVersion());
        });

        /* cleanup */
        lookupBooks.forEach(patchBookEntityMgr::delete);
    }

    private PatchBook clone(@NotNull PatchBook book) {
        return JsonUtils.deserialize(JsonUtils.serialize(book), PatchBook.class);
    }

    private void assertEquals(PatchBook book, PatchBook expectedBook) {
        if (expectedBook == null) {
            Assert.assertNull(book);
        } else {
            // use serialized form for verification
            Assert.assertEquals(JsonUtils.serialize(book), JsonUtils.serialize(expectedBook));
        }
    }

    @DataProvider(name = "patchBookQuery")
    private Object[][] providePatchBookQueryTestData() {
        return new Object[][] {
                { PatchBook.Type.Attribute, true },
                { PatchBook.Type.Attribute, false },
                { PatchBook.Type.Attribute, null },
                { PatchBook.Type.Lookup, true },
                { PatchBook.Type.Lookup, false },
                { PatchBook.Type.Lookup, null },
                { PatchBook.Type.Domain, true },
                { PatchBook.Type.Domain, false },
                { PatchBook.Type.Domain, null },
        };
    }
}
