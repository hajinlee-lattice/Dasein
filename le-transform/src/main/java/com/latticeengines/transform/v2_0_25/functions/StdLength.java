package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.ApprovedUsage;
import com.latticeengines.transform.exposed.metadata.Category;
import com.latticeengines.transform.exposed.metadata.FundamentalType;
import com.latticeengines.transform.exposed.metadata.StatisticalType;
import com.latticeengines.transform.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;

public class StdLength implements RealTimeTransform {

    private static final long serialVersionUID = 3315368854584210967L;

    public StdLength() {
    }

    public StdLength(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        String value = column == null ? null : String.valueOf(record.get(column));

        if (value.equals("null"))
            return 1;

        return calculateStdLength(value);
    }

    public static int calculateStdLength(String value) {
        if (StringUtils.isEmpty(value))
            return 1;
        if (value.trim().length() > 30)
            return 30;

        return value.trim().length();
    }

    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL);
        metadata.setCategory(Category.LEAD_INFORMATION);
        metadata.setFundamentalType(FundamentalType.NUMERIC);
        metadata.setStatisticalType(StatisticalType.RATIO);
        metadata.setTags(Tag.INTERNAL_TRANSFORM);
        metadata.setDisplayDiscretizationStrategy("{\"linear\": { \"minValue\":0,\"stepSize\":1,\"minSamples\":100," //
                + "\"minFreq\":0.01,\"maxBuckets\":5,\"maxPercentile\":1}}");
        return metadata;
    }
}
