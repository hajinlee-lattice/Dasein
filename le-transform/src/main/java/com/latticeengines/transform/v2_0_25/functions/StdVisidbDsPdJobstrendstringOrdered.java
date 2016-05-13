package com.latticeengines.transform.v2_0_25.functions;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.ApprovedUsage;
import com.latticeengines.transform.exposed.metadata.Category;
import com.latticeengines.transform.exposed.metadata.FundamentalType;
import com.latticeengines.transform.exposed.metadata.StatisticalType;
import com.latticeengines.transform.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;

public class StdVisidbDsPdJobstrendstringOrdered implements RealTimeTransform {

    private static final long serialVersionUID = -2693294029673945372L;

    public StdVisidbDsPdJobstrendstringOrdered() {
    }

    public StdVisidbDsPdJobstrendstringOrdered(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object n = record.get(column);

        if (n == null)
            return null;

        String s = n.toString().toLowerCase();

        return calculateStdVisidbDsPdJobstrendstringOrdered(s);
    }

    public static Integer calculateStdVisidbDsPdJobstrendstringOrdered(String hiringLevel) {
        if (StringUtils.isEmpty(hiringLevel))
            return null;

        HashMap<String, Integer> valueMap = new HashMap<String, Integer>();
        valueMap.put("moderately hiring", 1);
        valueMap.put("significantly hiring", 2);
        valueMap.put("aggressively hiring", 3);

        hiringLevel = hiringLevel.trim().toLowerCase();

        if (valueMap.containsKey(hiringLevel))
            return valueMap.get(hiringLevel);

        return 0;
    }

    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL);
        metadata.setCategory(Category.GROWTH_TRENDS);
        metadata.setFundamentalType(FundamentalType.NUMERIC);
        metadata.setStatisticalType(StatisticalType.ORDINAL);
        metadata.setTags(Tag.EXTERNAL_TRANSFORM);
        metadata.setDisplayDiscretizationStrategy("{\"linear\": { \"minValue\":0,\"stepSize\":1,\"minSamples\":100," //
                + "\"minFreq\":0.01,\"maxBuckets\":5,\"maxPercentile\":1}}");
        metadata.setDescription("Represents company\'s hiring activity within last 60 days. Values range from 1 (Moderately Hiring) to 3 (Aggressively Hiring)");
        metadata.setDisplayName("Recent Hiring Activity");
        return metadata;
    }
}
