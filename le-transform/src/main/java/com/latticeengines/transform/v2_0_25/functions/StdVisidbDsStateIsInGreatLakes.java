package com.latticeengines.transform.v2_0_25.functions;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.ApprovedUsage;
import com.latticeengines.transform.exposed.metadata.Category;
import com.latticeengines.transform.exposed.metadata.FundamentalType;
import com.latticeengines.transform.exposed.metadata.StatisticalType;
import com.latticeengines.transform.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;

public class StdVisidbDsStateIsInGreatLakes implements RealTimeTransform {

    private static final long serialVersionUID = -6195602149089977682L;

    static HashSet<String> valueMap = new HashSet<String>(Arrays.asList("OH", "MI", "IL", "WI", "IN"));

    public StdVisidbDsStateIsInGreatLakes() {
        
    }
    
    public StdVisidbDsStateIsInGreatLakes(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object n = record.get(column);

        if (n == null)
            return false;

        String s = n.toString().trim().toUpperCase();

        return getDSStateIsInGreatLakes(s);
    }

    public static Boolean getDSStateIsInGreatLakes(String state) {
        if (StringUtils.isEmpty(state))
            return false;

        return valueMap.contains(state);
    }

    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        metadata.setCategory(Category.LEAD_INFORMATION);
        metadata.setFundamentalType(FundamentalType.BOOLEAN);
        metadata.setStatisticalType(StatisticalType.NOMINAL);
        metadata.setDescription("Region: Great Lakes");
        metadata.setDisplayName("Region: Great Lakes");
        metadata.setTags(Tag.INTERNAL_TRANSFORM);
        return metadata;
    }
}
