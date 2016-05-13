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

public class StdVisidbDsTitleScope implements RealTimeTransform {

    private static final long serialVersionUID = -6009982200973336493L;
    @SuppressWarnings("rawtypes")
    private static LinkedHashMap mapTitleScope = null;

    public StdVisidbDsTitleScope() {

    }

    public StdVisidbDsTitleScope(String modelPath) {

    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object o = record.get(column);

        if (o == null)
            return "0.0";

        if (mapTitleScope == null) {
            mapTitleScope = new LinkedHashMap();
            mapTitleScope.put("Continental", "north america,emea,asia,africa,europ,south america");
            mapTitleScope.put("Global", "global,internaltional,worldwide");
            mapTitleScope.put("National", "us,national");
            mapTitleScope.put("Regional", "region,branch,territory,district,central,western,eastern,northern,southern");
        }

        String s = (String) o;

        return DSUtils.valueReturn(s, mapTitleScope);
    }

    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        metadata.setCategory(Category.LEAD_INFORMATION);
        metadata.setFundamentalType(FundamentalType.ALPHA);
        metadata.setStatisticalType(StatisticalType.NOMINAL);
        metadata.setDescription("Title Scope");
        metadata.setDisplayName("Title Scope");
        metadata.setTags(Tag.INTERNAL_TRANSFORM);
        return metadata;
    }

}
