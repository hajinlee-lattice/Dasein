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

public class StdVisidbDsFirstnameSameasLastname implements RealTimeTransform {

    private static final long serialVersionUID = -8266791987037795279L;

    public StdVisidbDsFirstnameSameasLastname() {
    }

    public StdVisidbDsFirstnameSameasLastname(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column1 = (String) arguments.get("column1");
        String column2 = (String) arguments.get("column2");

        String firstName = column1 == null ? null : String.valueOf(record.get(column1));
        String lastName = column2 == null ? null : String.valueOf(record.get(column2));

        if (firstName.equals("null") || lastName.equals("null"))
            return false;

        return calcualteStdVisidbDsFirstnameSameasLastname(firstName, lastName);
    }

    public static boolean calcualteStdVisidbDsFirstnameSameasLastname(String firstName, String lastName) {
        if (firstName.equals("null") || lastName.equals("null"))
            return false;

        if (StringUtils.isEmpty(firstName) || StringUtils.isEmpty(lastName))
            return false;

        firstName = firstName.trim().toLowerCase();
        lastName = lastName.trim().toLowerCase();

        if (firstName.equals(lastName))
            return true;

        return false;
    }

    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL);
        metadata.setCategory(Category.LEAD_INFORMATION);
        metadata.setFundamentalType(FundamentalType.BOOLEAN);
        metadata.setStatisticalType(StatisticalType.NOMINAL);
        metadata.setTags(Tag.INTERNAL_TRANSFORM);
        metadata.setDescription("Indicates that First Name is same as Last Name");
        metadata.setDisplayName("Identical First and Last Names");
        return metadata;
    }
}
