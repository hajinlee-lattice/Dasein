package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.StatisticalType;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.RealTimeTransform;

public class TitleLength implements RealTimeTransform {

    private static final long serialVersionUID = -7260877331948999620L;

    public TitleLength() {
    }

    public TitleLength(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object value = record.get(column);

        if (value == null)
            return 0;

        return value.toString().length();
    }

    @Override
    public Attribute getMetadata() {
        Attribute attr = new Attribute();
        attr.setApprovedUsage(ApprovedUsage.MODEL);
        attr.setCategory(Category.LEAD_INFORMATION);
        attr.setDataType(Integer.class.getSimpleName());
        attr.setFundamentalType(FundamentalType.NUMERIC);
        attr.setStatisticalType(StatisticalType.RATIO);
        attr.setTags(Tag.INTERNAL_TRANSFORM);
        return attr;
    }
}
