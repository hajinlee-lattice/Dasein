package com.latticeengines.pls.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;
import java.util.Set;

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
    }
}
