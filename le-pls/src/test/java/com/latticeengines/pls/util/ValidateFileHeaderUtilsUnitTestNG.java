package com.latticeengines.pls.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Set;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;

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
}
