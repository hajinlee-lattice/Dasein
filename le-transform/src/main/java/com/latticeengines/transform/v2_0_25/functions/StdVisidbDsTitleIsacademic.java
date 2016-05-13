package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;
import java.util.regex.Pattern;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.ApprovedUsage;
import com.latticeengines.transform.exposed.metadata.Category;
import com.latticeengines.transform.exposed.metadata.FundamentalType;
import com.latticeengines.transform.exposed.metadata.StatisticalType;
import com.latticeengines.transform.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;

public class StdVisidbDsTitleIsacademic implements RealTimeTransform {

    private static final long serialVersionUID = -3175023468680385610L;

    public StdVisidbDsTitleIsacademic() {
    }

    public StdVisidbDsTitleIsacademic(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object n = record.get(column);

        if (n == null)
            return false;

        String s = n.toString().toLowerCase();

        return (Pattern.matches(
                "(.*?\\b)student(.*)|(.*?\\b)researcher(.*)|(.*?\\b)professor(.*)|(.*)dev(.*)|(.*?\\b)programmer(.*)",
                s) && StdVisidbDsTitleLevel.calculateStdVisidbDsTitleLevel(s) == 0) ? true : false;
    }

    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        metadata.setCategory(Category.LEAD_INFORMATION);
        metadata.setFundamentalType(FundamentalType.BOOLEAN);
        metadata.setStatisticalType(StatisticalType.NOMINAL);
        metadata.setTags(Tag.INTERNAL_TRANSFORM);
        metadata.setDescription("Indicator for Academic Job Title");
        metadata.setDisplayName("Has Academic Title");
        return metadata;
    }
}
