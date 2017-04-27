package com.latticeengines.datacloud.dataflow.utils;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

public class FileParserUnitTestNG {
    private static final Log log = LogFactory.getLog(FileParserUnitTestNG.class);

    @Test(groups = "unit")
    public void testParseBomboraMetroCodes() {
        Map<String, List<NameLocation>> locationMap = FileParser.parseBomboraMetroCodes();
        Assert.assertNotNull(locationMap);
        for (String metroArea : locationMap.keySet()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Metro Area: " + metroArea + "\n");
            for (NameLocation location : locationMap.get(metroArea)) {
                sb.append(String.format("Country: %s, State: %s, City: %s\n", location.getCountry(),
                        location.getState(), location.getCity()));
            }
            log.info(sb.toString());
        }
    }
}
