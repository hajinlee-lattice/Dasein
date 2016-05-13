package com.latticeengines.transform.v2_0_25.functions;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.ApprovedUsage;
import com.latticeengines.transform.exposed.metadata.Category;
import com.latticeengines.transform.exposed.metadata.FundamentalType;
import com.latticeengines.transform.exposed.metadata.StatisticalType;
import com.latticeengines.transform.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;

public class StdVisidbDsPdFundingstageOrdered implements RealTimeTransform {

    private static final long serialVersionUID = -220002686310558617L;

    public StdVisidbDsPdFundingstageOrdered() {
    }

    public StdVisidbDsPdFundingstageOrdered(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object n = record.get(column);

        if (n == null)
            return null;

        String s = n.toString().toLowerCase();

        return calculateStdVisidbDsPdFundingstageOrdered(s);
    }

    public static Integer calculateStdVisidbDsPdFundingstageOrdered(String fundingStage) {
        if (StringUtils.isEmpty(fundingStage))
            return null;

        HashMap<String, Integer> valueMap = new HashMap<String, Integer>();
        valueMap.put("startup/seed", 1);
        valueMap.put("early stage", 2);
        valueMap.put("expansion", 3);
        valueMap.put("later stage", 4);

        fundingStage = fundingStage.trim().toLowerCase();

        if (valueMap.containsKey(fundingStage))
            return valueMap.get(fundingStage);

        return 0;
    }

    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL);
        metadata.setCategory(Category.GROWTH_TRENDS);
        metadata.setFundamentalType(FundamentalType.NUMERIC);
        metadata.setStatisticalType(StatisticalType.ORDINAL);
        metadata.setTags(Tag.EXTERNAL_TRANSFORM);
        metadata.setDisplayDiscretizationStrategy("{\"linear\": { \"minValue\":0,\"stepSize\":1,\"minSamples\":100," //
                + "\"minFreq\":0.01,\"maxBuckets\":5,\"maxPercentile\":1}}");
        metadata.setDescription("Represents funding stage.  Values are 1 (Startup/Seed), 2 (Early Stage), 3 (Expansion), and 4 (Later Stage)");
        metadata.setDisplayName("Funding Stage");
        return metadata;
    }
}
