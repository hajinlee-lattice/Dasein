package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.ApprovedUsage;
import com.latticeengines.transform.exposed.metadata.Category;
import com.latticeengines.transform.exposed.metadata.FundamentalType;
import com.latticeengines.transform.exposed.metadata.StatisticalType;
import com.latticeengines.transform.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;

public class DnBRevenueRangeOrdinal implements RealTimeTransform {

    private static final long serialVersionUID = 2101388118521954639L;

    public DnBRevenueRangeOrdinal() {
    }

    public DnBRevenueRangeOrdinal(String modelPath) {
    }

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");

        Object value = record.get(column);

        if (value == null) {
            return 1;
        }

        if (!(value instanceof String)) {
            return null;
        }

        String revenueRange = ((String) value).trim();

        switch (revenueRange) {
        case "0-1M":
            return 2;
        case "1-10M":
            return 3;
        case "11-50M":
            return 4;
        case "51-100M":
            return 5;
        case "101-250M":
            return 6;
        case "251-500M":
            return 7;
        case "501-1B":
            return 8;
        case "1-5B":
            return 9;
        case "5-10M":
            return 10;
        case ">10B":
            return 11;
        default:
            return 1;
        }
    }

    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL);
        metadata.setCategory(Category.FIRMOGRAPHICS);
        metadata.setFundamentalType(FundamentalType.NUMERIC);
        metadata.setStatisticalType(StatisticalType.INTERVAL);
        metadata.setDescription("Ordinal value for revenue range");
        metadata.setDisplayName("Revenue Range Ordinal");
        metadata.setTags(Tag.EXTERNAL_TRANSFORM);
        return metadata;
    }

}
