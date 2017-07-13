package com.latticeengines.datacloud.dataflow.utils;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

public class FileParserUnitTestNG {
    private static final Logger log = LoggerFactory.getLogger(FileParserUnitTestNG.class);

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

    @Test(groups = "unit")
    public void testParseBomboraIntent() {
        Map<String, Map<Range<Integer>, String>> intentMap = FileParser.parseBomboraIntent();
        Assert.assertNotNull(intentMap);
        Assert.assertEquals(intentMap.size(), 3);
        for (String bucketCode : intentMap.keySet()) {
            Map<Range<Integer>, String> compoScoreIntent = intentMap.get(bucketCode);
            for (Range<Integer> range : compoScoreIntent.keySet()) {
                log.info(String.format("BucketCode=%s, CompoScore range=(%d,%d), Intent=%s", bucketCode,
                        range.getMinimum(), range.getMaximum(), compoScoreIntent.get(range)));
            }
        }
    }
}
