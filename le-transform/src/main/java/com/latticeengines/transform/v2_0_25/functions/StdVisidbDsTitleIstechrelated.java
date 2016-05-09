package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;
import java.util.regex.Pattern;

import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.StatisticalType;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.RealTimeTransform;

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
    public Attribute getMetadata() {
        Attribute attr = new Attribute();
        attr.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        attr.setCategory(Category.LEAD_INFORMATION);
        attr.setFundamentalType(FundamentalType.BOOLEAN);
        attr.setStatisticalType(StatisticalType.ORDINAL);
        attr.setTags(Tag.INTERNAL_TRANSFORM);
        attr.setDescription("Indicator for Technical Job Title");
        attr.setDisplayName("Has Technical Title");
        return attr;
    }
}
