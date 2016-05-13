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
import com.latticeengines.transform.v2_0_25.common.DSUtils;

public class StdVisidbDsTitleLevelCategorical implements RealTimeTransform {

    private static final long serialVersionUID = 8526302676390701013L;

    public StdVisidbDsTitleLevelCategorical() {
        
    }
    
    public StdVisidbDsTitleLevelCategorical(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object n = record.get(column);

        if (n == null)
            return "0.0";

        String s = n.toString().toLowerCase();

        return getDSTitleLevelCategorical(s);
    }

    public static String getDSTitleLevelCategorical(String title) {
        if (StringUtils.isEmpty(title))
            return "Staff";

        title = title.trim().toLowerCase();

        if (DSUtils.hasUnUsualChar(title))
            return "";

        if (title.contains("vice"))
            return "Vice President";

        if (title.contains("director"))
            return "Director";

        if (title.contains("manager"))
            return "Manager";

        return "Staff";
    }

    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        metadata.setCategory(Category.LEAD_INFORMATION);
        metadata.setFundamentalType(FundamentalType.ALPHA);
        metadata.setStatisticalType(StatisticalType.NOMINAL);
        metadata.setDescription("Title Category");
        metadata.setDisplayName("Title Category");
        metadata.setTags(Tag.INTERNAL_TRANSFORM);
        return metadata;
    }
}
