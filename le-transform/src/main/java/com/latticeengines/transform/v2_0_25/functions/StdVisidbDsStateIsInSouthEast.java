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

public class StdVisidbDsStateIsInSouthEast implements RealTimeTransform {

    private static final long serialVersionUID = 2399593789532031842L;
    static HashSet<String> valueMap = new HashSet<String>(
            Arrays.asList("VA", "MS", "LA", "NC", "AL", "TN", "WV", "AR", "GA", "SC", "KY", "FL"));

    public StdVisidbDsStateIsInSouthEast() {
        
    }
    
    public StdVisidbDsStateIsInSouthEast(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object n = record.get(column);

        if (n == null)
            return false;

        String s = n.toString().trim().toUpperCase();

        return getDSStateIsInSouthEast(s);
    }

    public static Boolean getDSStateIsInSouthEast(String state) {
        if (StringUtils.isEmpty(state))
            return false;

        return valueMap.contains(state);
    }

    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL);
        metadata.setCategory(Category.LEAD_INFORMATION);
        metadata.setFundamentalType(FundamentalType.BOOLEAN);
        metadata.setStatisticalType(StatisticalType.NOMINAL);
        metadata.setDescription("Region: Southeast");
        metadata.setDisplayName("Region: Southeast");
        metadata.setTags(Tag.INTERNAL_TRANSFORM);
        return metadata;
    }
}
