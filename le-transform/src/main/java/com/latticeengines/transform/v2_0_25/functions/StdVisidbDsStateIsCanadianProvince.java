package com.latticeengines.transform.v2_0_25.functions;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.StatisticalType;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.RealTimeTransform;

public class StdVisidbDsStateIsCanadianProvince implements RealTimeTransform {

    private static final long serialVersionUID = -5597411780669749100L;
    static HashSet<String> valueMap = new HashSet<String>(
            Arrays.asList("ON", "AB", "NL", "MB", "NB", "BC", "YT", "SK", "QC", "PE", "NS", "NT", "NU"));

    public StdVisidbDsStateIsCanadianProvince() {
        
    }
    
    public StdVisidbDsStateIsCanadianProvince(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object n = record.get(column);

        if (n == null)
            return false;

        String s = n.toString().trim().toUpperCase();

        return getDSStateIsCanadianProvince(s);
    }

    public static Boolean getDSStateIsCanadianProvince(String state) {
        if (StringUtils.isEmpty(state))
            return false;

        return valueMap.contains(state);
    }

    @Override
    public Attribute getMetadata() {
        Attribute attribute = new Attribute();
        attribute.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        attribute.setCategory(Category.LEAD_INFORMATION);
        attribute.setFundamentalType(FundamentalType.BOOLEAN);
        attribute.setStatisticalType(StatisticalType.NOMINAL);
        attribute.setDescription("Region: Canadian Province");
        attribute.setDisplayName("Region: Canadian Province");
        attribute.setTags(Tag.INTERNAL_TRANSFORM);
        return attribute;
    }
}
