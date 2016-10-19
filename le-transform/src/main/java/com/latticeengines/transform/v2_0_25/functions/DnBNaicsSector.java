package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import com.latticeengines.transform.exposed.metadata.ApprovedUsage;
import com.latticeengines.transform.exposed.metadata.Category;
import com.latticeengines.transform.exposed.metadata.FundamentalType;
import com.latticeengines.transform.exposed.metadata.StatisticalType;
import com.latticeengines.transform.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;

public class DnBNaicsSector extends Substring {

    private static final long serialVersionUID = -7537875837591495149L;

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String naicsCode = getValueToSubstring(arguments, record);
        
        try {
            if (naicsCode != null) {
                return transform(0, 2, naicsCode);
            }
        } catch (Exception e) {
            return null;
        }
        
        return naicsCode;
    }
    

    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL);
        metadata.setCategory(Category.FIRMOGRAPHICS);
        metadata.setFundamentalType(FundamentalType.ALPHA);
        metadata.setStatisticalType(StatisticalType.NOMINAL);
        metadata.setDescription("NAICS Sector. There are 20 NAICS sectors.");
        metadata.setDisplayName("NAICS Sector");
        metadata.setTags(Tag.EXTERNAL_TRANSFORM);
        return metadata;
    }

}
