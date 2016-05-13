package com.latticeengines.transform.v2_0_25.functions;

import java.util.LinkedHashMap;
import java.util.Map;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.ApprovedUsage;
import com.latticeengines.transform.exposed.metadata.Category;
import com.latticeengines.transform.exposed.metadata.FundamentalType;
import com.latticeengines.transform.exposed.metadata.StatisticalType;
import com.latticeengines.transform.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;
import com.latticeengines.transform.v2_0_25.common.DSUtils;

public class StdVisidbDsTitleChannel implements RealTimeTransform {

    private static final long serialVersionUID = -2669053366227213576L;
    @SuppressWarnings("rawtypes")
    private static LinkedHashMap mapTitleChannel = null;
    
    public StdVisidbDsTitleChannel() {
        
    }

    public StdVisidbDsTitleChannel(String modelPath) {

    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object o = record.get(column);

        if (o == null)
            return "0.0";

        if (mapTitleChannel == null) {
            mapTitleChannel = new LinkedHashMap();
            mapTitleChannel.put("Consumer", "consumer,retail");
            mapTitleChannel.put("Government", "government");
            mapTitleChannel.put("Corporate", "enterprise,corporate");
        }

        String s = (String) o;

        return DSUtils.valueReturn(s, mapTitleChannel);
    }

    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        metadata.setCategory(Category.LEAD_INFORMATION);
        metadata.setFundamentalType(FundamentalType.ALPHA);
        metadata.setStatisticalType(StatisticalType.NOMINAL);
        metadata.setDescription("Title Channel");
        metadata.setDisplayName("Title Channel");
        metadata.setTags(Tag.INTERNAL_TRANSFORM);
        
        return metadata;
    }

}
