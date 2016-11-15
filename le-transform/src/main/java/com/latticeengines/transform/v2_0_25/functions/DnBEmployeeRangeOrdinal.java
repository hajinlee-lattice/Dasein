package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.ApprovedUsage;
import com.latticeengines.transform.exposed.metadata.Category;
import com.latticeengines.transform.exposed.metadata.FundamentalType;
import com.latticeengines.transform.exposed.metadata.StatisticalType;
import com.latticeengines.transform.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;

public class DnBEmployeeRangeOrdinal implements RealTimeTransform {

    private static final long serialVersionUID = -361652189091168052L;

    public DnBEmployeeRangeOrdinal() {
    }

    public DnBEmployeeRangeOrdinal(String modelPath) {
    }

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");

        Object value = record.get(column);

        if (value == null) {
            return -10;
        }

        if (!(value instanceof String)) {
            return null;
        }

        String employeeRange = ((String) value).trim();

        switch (employeeRange) {
        case "0":
            return 0;
        case "1-10":
            return 10;
        case "11-50":
            return 20;
        case "51-100":
            return 30;
        case "101-200":
            return 40;
        case "201-500":
            return 50;
        case "501-1000":
            return 60;
        case "1001-2500":
            return 70;
        case "2501-5000":
            return 80;
        case "5001-10,000":
            return 90;
        case ">10,000":
            return 100;
        default:
            return -100;
        }
    }

    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL);
        metadata.setCategory(Category.FIRMOGRAPHICS);
        metadata.setFundamentalType(FundamentalType.NUMERIC);
        metadata.setStatisticalType(StatisticalType.ORDINAL);
        metadata.setDescription("Ordinal value for employee range");
        metadata.setDisplayName("Employee Range Ordinal");
        metadata.setTags(Tag.EXTERNAL_TRANSFORM);
        return metadata;
    }

}
