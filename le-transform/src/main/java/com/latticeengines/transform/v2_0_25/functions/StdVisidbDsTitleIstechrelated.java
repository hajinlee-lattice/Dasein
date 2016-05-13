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

public class StdVisidbDsTitleIstechrelated implements RealTimeTransform {

    private static final long serialVersionUID = -8583683317592691766L;

    public StdVisidbDsTitleIstechrelated() {
    }

    public StdVisidbDsTitleIstechrelated(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object n = record.get(column);

        if (n == null)
            return false;

        String s = n.toString().toLowerCase();

        return Pattern.matches("(.*?\\b)eng(.*)|(.*?\\b)tech(.*)|(.*?\\b)info(.*)|(.*)dev(.*)", s) ? true : false;
    }

    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        metadata.setCategory(Category.LEAD_INFORMATION);
        metadata.setFundamentalType(FundamentalType.BOOLEAN);
        metadata.setStatisticalType(StatisticalType.ORDINAL);
        metadata.setTags(Tag.INTERNAL_TRANSFORM);
        metadata.setDescription("Indicator for Technical Job Title");
        metadata.setDisplayName("Has Technical Title");
        return metadata;
    }
}
