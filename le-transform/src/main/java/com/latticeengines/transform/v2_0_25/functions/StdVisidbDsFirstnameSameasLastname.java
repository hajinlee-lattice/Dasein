package com.latticeengines.transform.v2_0_25.functions;


import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.StatisticalType;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.RealTimeTransform;

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
    public Attribute getMetadata() {
        Attribute attr = new Attribute();
        attr.setApprovedUsage(ApprovedUsage.MODEL);
        attr.setCategory(Category.LEAD_INFORMATION);
        attr.setFundamentalType(FundamentalType.BOOLEAN);
        attr.setStatisticalType(StatisticalType.NOMINAL);
        attr.setTags(Tag.INTERNAL_TRANSFORM);
        attr.setDescription("Indicates that First Name is same as Last Name");
        attr.setDisplayName("Identical First and Last Names");
        return attr;
    }
}
