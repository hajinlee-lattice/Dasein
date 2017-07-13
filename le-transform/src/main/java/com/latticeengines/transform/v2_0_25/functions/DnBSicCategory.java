package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import com.latticeengines.transform.exposed.metadata.ApprovedUsage;
import com.latticeengines.transform.exposed.metadata.Category;
import com.latticeengines.transform.exposed.metadata.FundamentalType;
import com.latticeengines.transform.exposed.metadata.StatisticalType;
import com.latticeengines.transform.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;

public class DnBSicCategory extends Substring {

    private static final long serialVersionUID = -2916398769800590028L;

    public DnBSicCategory() {
    }

    public DnBSicCategory(String modelPath) {
    }

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String sicCode = getValueToSubstring(arguments, record);

        try {
            if (sicCode != null) {
                return transform(0, 2, sicCode);
            }
        } catch (Exception e) {
            return null;
        }

        return sicCode;
    }


    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL);
        metadata.setCategory(Category.FIRMOGRAPHICS);
        metadata.setFundamentalType(FundamentalType.ALPHA);
        metadata.setStatisticalType(StatisticalType.NOMINAL);
        metadata.setDescription("SIC Logger. There are 99 categories");
        metadata.setDisplayName("SIC Logger");
        metadata.setTags(Tag.EXTERNAL_TRANSFORM);
        return metadata;
    }

}
