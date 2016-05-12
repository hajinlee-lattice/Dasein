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

public class StdVisidbDsTitleScope implements RealTimeTransform {

    private static final long serialVersionUID = -6009982200973336493L;
    private static OrderedMap mapTitleScope = null;

    public StdVisidbDsTitleScope() {
        
    }
    
    public StdVisidbDsTitleScope(String modelPath) {

    }

    @SuppressWarnings("unchecked")
    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object o = record.get(column);

        if (o == null)
            return 0.0;

        if (mapTitleScope == null) {
            mapTitleScope = new LinkedMap();
            mapTitleScope.put("Continental", "north america,emea,asia,africa,europ,south america");
            mapTitleScope.put("Global", "global,internaltional,worldwide");
            mapTitleScope.put("National", "us,national");
            mapTitleScope.put("Regional", "region,branch,territory,district,central,western,eastern,northern,southern");
        }

        String s = (String) o;

        return DSUtils.valueReturn(s, mapTitleScope);
    }

    @Override
    public Attribute getMetadata() {
        Attribute attribute = new Attribute();
        attribute.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        attribute.setCategory(Category.LEAD_INFORMATION);
        attribute.setFundamentalType(FundamentalType.ALPHA);
        attribute.setStatisticalType(StatisticalType.NOMINAL);
        attribute.setDescription("Title Scope");
        attribute.setDisplayName("Title Scope");
        attribute.setTags(Tag.INTERNAL_TRANSFORM);
        return attribute;
    }

}
