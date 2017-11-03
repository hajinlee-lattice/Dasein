package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.ApprovedUsage;
import com.latticeengines.transform.exposed.metadata.Category;
import com.latticeengines.transform.exposed.metadata.FundamentalType;
import com.latticeengines.transform.exposed.metadata.StatisticalType;
import com.latticeengines.transform.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;

public class StdVisidbDsEmailPrefixlength implements RealTimeTransform {

    private static final long serialVersionUID = 664716781231084878L;

    public StdVisidbDsEmailPrefixlength() {
        
    }
    
    public StdVisidbDsEmailPrefixlength(String modelPath) {

    }

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object o = record.get(column);

        if (o == null)
            return 0;

        String s = (String) o;

        if (s.indexOf("@") < 0)
            return 0;

        return s.indexOf("@");
    }

    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL);
        metadata.setCategory(Category.LEAD_INFORMATION);
        metadata.setFundamentalType(FundamentalType.NUMERIC);
        metadata.setStatisticalType(StatisticalType.RATIO);
        metadata.setDescription("Email Prefix Length");
        metadata.setDisplayName("Email Prefix Length");
        metadata.setTags(Tag.INTERNAL_TRANSFORM);
        return metadata;
    }
}
