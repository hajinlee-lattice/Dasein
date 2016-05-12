package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.StatisticalType;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.RealTimeTransform;

public class StdVisidbDsEmailIsInvalid implements RealTimeTransform {

    private static final long serialVersionUID = -2544730034184720534L;

    public StdVisidbDsEmailIsInvalid() {
        
    }
    
    public StdVisidbDsEmailIsInvalid(String modelPath) {

    }

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object o = record.get(column);

        if (o == null)
            return true;

        String s = (String) o;

        if (s.length() < 5)
            return true;

        if (!s.contains("@"))
            return true;

        return false;
    }

    @Override
    public Attribute getMetadata() {
        Attribute attribute = new Attribute();
        attribute.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        attribute.setCategory(Category.LEAD_INFORMATION);
        attribute.setFundamentalType(FundamentalType.BOOLEAN);
        attribute.setStatisticalType(StatisticalType.NOMINAL);
        attribute.setDescription("Invalid Email");
        attribute.setDisplayName("Invalid Email");
        attribute.setTags(Tag.INTERNAL_TRANSFORM);
        return attribute;
    }
}
