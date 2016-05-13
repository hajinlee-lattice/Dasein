package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.ApprovedUsage;
import com.latticeengines.transform.exposed.metadata.Category;
import com.latticeengines.transform.exposed.metadata.FundamentalType;
import com.latticeengines.transform.exposed.metadata.StatisticalType;
import com.latticeengines.transform.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;

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
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        metadata.setCategory(Category.LEAD_INFORMATION);
        metadata.setFundamentalType(FundamentalType.NUMERIC);
        metadata.setStatisticalType(StatisticalType.ORDINAL);
        metadata.setDescription("Name Length");
        metadata.setDisplayName("Name Length");
        metadata.setTags(Tag.INTERNAL_TRANSFORM);

        return metadata;
    }
}
