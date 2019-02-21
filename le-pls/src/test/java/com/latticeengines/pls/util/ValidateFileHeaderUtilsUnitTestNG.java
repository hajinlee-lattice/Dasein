package com.latticeengines.pls.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.validation.ReservedField;

public class ValidateFileHeaderUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testGetCSVFileHeader() throws FileNotFoundException {
        URL topPredictorCSVFileUrl = ClassLoader
                .getSystemResource("com/latticeengines/pls/functionalframework/topPredictor_model.csv");
        File csvFile = new File(topPredictorCSVFileUrl.getFile());
        InputStream stream = new FileInputStream(csvFile);
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        Set<String> headers = ValidateFileHeaderUtils.getCSVHeaderFields(stream, closeableResourcePool);
        assertTrue(!headers.isEmpty());
        assertEquals(headers.size(), 17);
        try {
            closeableResourcePool.close();
        } catch (IOException e) {
            throw new RuntimeException("Problem when closing the pool", e);
        }
    }

    @Test(groups = "unit")
    public void testValidateCSVFileHeader() throws FileNotFoundException {
        URL topPredictorCSVFileUrl = ClassLoader.getSystemResource("com/latticeengines/pls/util/wrong_format_file.csv");
        File csvFile = new File(topPredictorCSVFileUrl.getFile());
        InputStream stream = new FileInputStream(csvFile);
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        Set<String> headers = ValidateFileHeaderUtils.getCSVHeaderFields(stream, closeableResourcePool);
        boolean thrownNotException = true;
        try {
            ValidateFileHeaderUtils.checkForHeaderFormat(headers);
            thrownNotException = false;
        } catch (Exception e) {
            assertTrue(thrownNotException, "Should have thrown exception");
        }
        try {
            closeableResourcePool.close();
        } catch (IOException e) {
            throw new RuntimeException("Problem when closing the pool", e);
        }
    }

    /**
     * Test {@link ValidateFileHeaderUtils#checkForLongHeaders(Set)}
     * @throws FileNotFoundException should not be thrown
     */
    @Test(groups = "unit", dataProvider = "testLongCSVFileHeader")
    public void testValidateLongCSVFileHeader(String filename, Integer expectedHeaderSize, Boolean shouldPass)
            throws FileNotFoundException {
        URL topPredictorCSVFileUrl = ClassLoader.getSystemResource(filename);
        File csvFile = new File(topPredictorCSVFileUrl.getFile());
        InputStream stream = new FileInputStream(csvFile);
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        try {
            Set<String> headers = ValidateFileHeaderUtils.getCSVHeaderFields(stream, closeableResourcePool);
            // check if the header is parsed correctly
            assertEquals(Integer.valueOf(headers.size()), expectedHeaderSize);
            // check if any header is too long
            ValidateFileHeaderUtils.checkForLongHeaders(headers);
            if (!shouldPass) {
                fail("Should have thrown exception");
            }
        } catch (LedpException e) {
            if (shouldPass) {
                fail("Should not have thrown exception");
            } else {
                assertNotNull(e);
                assertEquals(e.getCode(), LedpCode.LEDP_18188);
            }
        } finally {
            try {
                closeableResourcePool.close();
            } catch (IOException e) {
                fail("Problem when closing the pool", e);
            }
        }
    }

    @Test(groups = "unit")
    public void testDuplicateHeaders() throws IOException {
        URL topPredictorCSVFileUrl = ClassLoader
                .getSystemResource("com/latticeengines/pls/functionalframework/duplicate_headers.csv");
        File csvFile = new File(topPredictorCSVFileUrl.getFile());
        InputStream stream = new FileInputStream(csvFile);
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        LedpException thrown = null;
        try {
            ValidateFileHeaderUtils.getCSVHeaderFields(stream, closeableResourcePool);
        } catch (LedpException e) {
            thrown = e;
        }
        assertNotNull(thrown);
        assertEquals(thrown.getCode(), LedpCode.LEDP_18109);
    }

    @Test(groups = "unit")
    public void testConvertFieldNameToAvroFriendlyFormat() {
        String malformedName = "2name?*wer23";
        String expectedString = "avro_2name__wer23";
        assertEquals(ValidateFileHeaderUtils.convertFieldNameToAvroFriendlyFormat(malformedName), expectedString);

        Table table = new Table();
        Attribute duplicateAttribute1 = new Attribute();
        duplicateAttribute1.setName(ValidateFileHeaderUtils.convertFieldNameToAvroFriendlyFormat("1-200"));
        Attribute duplicateAttribute2 = new Attribute();
        duplicateAttribute2.setName(ValidateFileHeaderUtils.convertFieldNameToAvroFriendlyFormat("avro_1_200"));
        Attribute duplicateAttribute3 = new Attribute();
        duplicateAttribute3.setName(ValidateFileHeaderUtils.convertFieldNameToAvroFriendlyFormat("1_200"));
        table.addAttribute(duplicateAttribute1);
        table.addAttribute(duplicateAttribute2);
        table.addAttribute(duplicateAttribute3);
        table.deduplicateAttributeNames();

        final List<String> expectedNames = new ArrayList<String>();
        expectedNames.add("avro_1_200");
        expectedNames.add("avro_1_200_1");
        expectedNames.add("avro_1_200_2");
        boolean allExpectedNamesAreAvroFriendly = Iterables.all(expectedNames, new Predicate<String>() {
            @Override
            public boolean apply(String input) {
                return AvroUtils.isAvroFriendlyFieldName(input);
            }
        });
        assertTrue(allExpectedNamesAreAvroFriendly);

        boolean convertAndDedupeAreRight = Iterables.any(table.getAttributes(), new Predicate<Attribute>() {
            @Override
            public boolean apply(Attribute input) {
                return expectedNames.contains(input.getName());
            }
        });
        assertTrue(convertAndDedupeAreRight);
    }

    @Test(groups = "unit")
    public void testisAvroFriendlyFieldName() {
        String malformedName = "2name?*wer23";
        assertFalse(AvroUtils.isAvroFriendlyFieldName(malformedName));
        String correctformedName = "avro_2name_23";
        assertTrue(AvroUtils.isAvroFriendlyFieldName(correctformedName));
    }

    @Test(groups = "unit", dataProvider = "reservedNames")
    public void validateReserverdWordsInHeaders(String reservedName) throws IOException {
        LedpException thrown = null;
        try {
            ValidateFileHeaderUtils.checkForReservedHeaders("file",
                    Sets.newHashSet(new String[] { "a", "b", reservedName, "c" }),
                    Collections.singletonList(reservedName), Collections.emptyList());
        } catch (LedpException e) {
            thrown = e;
        }
        assertNotNull(thrown);
        assertEquals(thrown.getCode(), LedpCode.LEDP_18122);
    }

    @Test(groups = "unit", dataProvider = "reservedBeginings", enabled = false)
    public void validateReservedBeginingsInHeaders(String reservedBeginings) throws IOException {
        LedpException thrown = null;
        String reservedName = reservedBeginings + "01";
        try {
            ValidateFileHeaderUtils.checkForReservedHeaders("file",
                    Sets.newHashSet(new String[] { "a", "b", reservedName, "c" }),
                    Collections.emptyList(), Collections.singleton(reservedName));
        } catch (LedpException e) {
            thrown = e;
        }
        assertNotNull(thrown);
        // assertEquals(thrown.getCode(), LedpCode.LEDP_18183);
    }

    @DataProvider(name = "reservedNames")
    public Object[][] reservedNames() {
        return new Object[][] { new Object[] { ReservedField.Percentile.displayName },
                new Object[] { ReservedField.Rating.displayName } };
    }

    @DataProvider(name = "reservedBeginings")
    public Object[][] reservedBeginings() {
        return new Object[][] { new Object[] { DataCloudConstants.CEAttr }, new Object[] { DataCloudConstants.EAttr } };
    }

    /**
     * Generate the testing data for {@link ValidateFileHeaderUtils#checkForLongHeaders(Set)}
     * the
     * @return array of testing data arrays, each data array is in the following format
     * the first item is the resource csv file name
     * the second item is the expected number of headers
     * the third item is a flag to specify whether the test should pass
     */
    @DataProvider(name = "testLongCSVFileHeader")
    public Object[][] provideTestLongCSVHeaderFile() {
        return new Object[][] {
                // invalid csv files
                { "com/latticeengines/pls/util/long_headers.csv", 5, false },
                // valid csv files
                { "com/latticeengines/pls/functionalframework/topPredictor_model.csv", 17, true }
        };
    }

}
