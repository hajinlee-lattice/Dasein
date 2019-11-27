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
import java.util.Collections;
import java.util.Set;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
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
                { "com/latticeengines/pls/util/long_headers.csv", 6, false },
                // valid csv files
                { "com/latticeengines/pls/functionalframework/topPredictor_model.csv", 17, true }
        };
    }

}
