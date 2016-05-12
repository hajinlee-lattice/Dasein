package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import org.apache.commons.collections.OrderedMap;
import org.apache.commons.collections.map.LinkedMap;

import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.StatisticalType;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.v2_0_25.common.DSUtils;

public class StdVisidbDsTitleChannel implements RealTimeTransform {

    private static final long serialVersionUID = -2669053366227213576L;
    private static OrderedMap mapTitleChannel = null;
    
    public StdVisidbDsTitleChannel() {
        
    }

    public StdVisidbDsTitleChannel(String modelPath) {

    }

    @SuppressWarnings("unchecked")
    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object o = record.get(column);

        if (o == null)
            return "0.0";

        if (mapTitleChannel == null) {
            mapTitleChannel = new LinkedMap();
            mapTitleChannel.put("Consumer", "consumer,retail");
            mapTitleChannel.put("Government", "government");
            mapTitleChannel.put("Corporate", "enterprise,corporate");
        }

        String s = (String) o;

        return DSUtils.valueReturn(s, mapTitleChannel);
    }

    @Override
    public Attribute getMetadata() {
        Attribute attribute = new Attribute();
        attribute.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        attribute.setCategory(Category.LEAD_INFORMATION);
        attribute.setFundamentalType(FundamentalType.ALPHA);
        attribute.setStatisticalType(StatisticalType.NOMINAL);
        attribute.setDescription("Title Channel");
        attribute.setDisplayName("Title Channel");
        attribute.setTags(Tag.INTERNAL_TRANSFORM);
        
        return attribute;
    }

}
