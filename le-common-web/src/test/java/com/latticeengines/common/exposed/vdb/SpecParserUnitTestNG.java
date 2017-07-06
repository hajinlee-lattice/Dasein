package com.latticeengines.common.exposed.vdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.Map;
import java.util.Scanner;

import org.testng.Assert;
import org.testng.annotations.Test;

public class SpecParserUnitTestNG {

    @Test(groups = "unit")
    public void testGetSegmentMap() throws FileNotFoundException, SpecParseException {
        URL url = ClassLoader.getSystemResource("com/latticeengines/common/exposed/vdb/spec.maude");
        File specFile = new File(url.getFile());
        Scanner scanner = null;
        try {
            scanner = new Scanner(specFile, "UTF-8");
            String spec = scanner.useDelimiter("\\A").next();
            SpecParser sp = new SpecParser(spec);
            Map<String, String> map = sp.getSegmentMap();

            Assert.assertTrue(map.containsKey("default"));
            Assert.assertTrue(map.containsKey("DefnSegment_Spain"));
            Assert.assertTrue(map.containsKey("DefnSegment_US"));
            Assert.assertEquals(map.get("default"), "Unknown");
            Assert.assertEquals(map.get("DefnSegment_Spain"), "GUID-Spain");
            Assert.assertEquals(map.get("DefnSegment_US"), "GUID-US");
            
        } finally {
            scanner.close();
        }
    }

    @Test(groups = "unit")
    public void testGetTemplate() throws FileNotFoundException, SpecParseException {
        URL url = ClassLoader.getSystemResource("com/latticeengines/common/exposed/vdb/spec2.maude");
        File specFile = new File(url.getFile());
        Scanner scanner = null;
        try {
            scanner = new Scanner(specFile, "UTF-8");
            String spec = scanner.useDelimiter("\\A").next();
            SpecParser sp = new SpecParser(spec);
            Assert.assertEquals(sp.getTemplate(), "PLS SFDC Template:1.4.1");
        } finally {
            scanner.close();
        }
        
    }

}
