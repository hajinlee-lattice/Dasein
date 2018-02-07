package com.latticeengines.domain.exposed.util;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MapUtils;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.CoverageInfo;
import com.latticeengines.domain.exposed.pls.RatingBucketCoverage;
import com.latticeengines.domain.exposed.pls.RatingEngine;

public class RatingEngineUtils {

    public static CoverageInfo getCoverageInfo(RatingEngine ratingEngine) {
        CoverageInfo coverageInfo = new CoverageInfo();
        MetadataSegment segment = ratingEngine.getSegment();
        if (segment != null) {
            coverageInfo.setAccountCount(segment.getAccounts());
            coverageInfo.setContactCount(segment.getContacts());
        }
        if (MapUtils.isNotEmpty(ratingEngine.getCountsAsMap())) {
            Map<String, Long> ratingCounts = new TreeMap<>(ratingEngine.getCountsAsMap());
            List<RatingBucketCoverage> coverages = ratingCounts.entrySet().stream().map(entry -> {
                RatingBucketCoverage bktCvg = new RatingBucketCoverage();
                bktCvg.setBucket(entry.getKey());
                bktCvg.setCount(entry.getValue());
                return bktCvg;
            }).collect(Collectors.toList());
            coverageInfo.setBucketCoverageCounts(coverages);
        }
        return coverageInfo;
    }

}
