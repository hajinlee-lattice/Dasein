package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.StatisticalType;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.RealTimeTransform;

public class StdVisidbDsNamelength implements RealTimeTransform {

    private static final long serialVersionUID = 1437270491579294595L;

    public StdVisidbDsNamelength() {
        
    }
    
    public StdVisidbDsNamelength(String modelPath) {

    }

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column1 = (String) arguments.get("column1");
        String column2 = (String) arguments.get("column2");

        Object firstName = record.get(column1);
        Object lastName = record.get(column2);

        if (firstName == null)
            firstName = "";
        if (lastName == null)
            lastName = "";

        return firstName.toString().length() + lastName.toString().length();
    }

    @Override
    public Attribute getMetadata() {
        Attribute attribute = new Attribute();
        attribute.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        attribute.setCategory(Category.LEAD_INFORMATION);
        attribute.setFundamentalType(FundamentalType.NUMERIC);
        attribute.setStatisticalType(StatisticalType.ORDINAL);
        attribute.setDescription("Name Length");
        attribute.setDisplayName("Name Length");
        attribute.setTags(Tag.INTERNAL_TRANSFORM);
        
        return attribute;
    }
}
