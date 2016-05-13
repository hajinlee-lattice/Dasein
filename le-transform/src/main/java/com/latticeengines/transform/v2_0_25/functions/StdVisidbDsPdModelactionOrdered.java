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

public class StdVisidbDsPdModelactionOrdered implements RealTimeTransform {

    private static final long serialVersionUID = 8358150425323914737L;

    public StdVisidbDsPdModelactionOrdered() {
    }

    public StdVisidbDsPdModelactionOrdered(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        String modelAction = column == null ? null : String.valueOf(record.get(column));

        if (modelAction.equals("null"))
            return null;

        return calculateStdVisidbDsPdModelactionOrdered(modelAction);
    }

    public static Integer calculateStdVisidbDsPdModelactionOrdered(String modelAction) {
        if (StringUtils.isEmpty(modelAction))
            return null;

        modelAction = modelAction.trim().toLowerCase();

        HashMap<String, Integer> valueMap = new HashMap<String, Integer>();
        valueMap.put("low risk", 1);
        valueMap.put("low-medium risk", 2);
        valueMap.put("medium risk", 3);
        valueMap.put("medium-high risk", 4);
        valueMap.put("high risk", 5);
        valueMap.put("recent bankruptcy on file", 6);

        if (valueMap.containsKey(modelAction))
            return valueMap.get(modelAction);

        return 0;
    }

    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL);
        metadata.setCategory(Category.FIRMOGRAPHICS);
        metadata.setFundamentalType(FundamentalType.NUMERIC);
        metadata.setStatisticalType(StatisticalType.ORDINAL);
        metadata.setTags(Tag.EXTERNAL_TRANSFORM);
        metadata.setDisplayDiscretizationStrategy("{\"linear\": { \"minValue\":0,\"stepSize\":1,\"minSamples\":100," //
                + "\"minFreq\":0.01,\"maxBuckets\":5,\"maxPercentile\":1}}");
        metadata.setDescription("Represents company\'s hiring activity within last 60 days. Values range from 1 (Moderately Hiring) to 3 (Aggressively Hiring)");
        metadata.setDisplayName("Credit Risk Level");
        return metadata;
    }
}
