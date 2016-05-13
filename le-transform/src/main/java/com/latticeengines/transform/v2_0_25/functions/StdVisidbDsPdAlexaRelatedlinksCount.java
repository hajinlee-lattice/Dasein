package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.ApprovedUsage;
import com.latticeengines.transform.exposed.metadata.Category;
import com.latticeengines.transform.exposed.metadata.FundamentalType;
import com.latticeengines.transform.exposed.metadata.StatisticalType;
import com.latticeengines.transform.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;

public class StdVisidbDsPdAlexaRelatedlinksCount implements RealTimeTransform {

    private static final long serialVersionUID = -9101655382573322702L;

    public StdVisidbDsPdAlexaRelatedlinksCount() {
    }

    public StdVisidbDsPdAlexaRelatedlinksCount(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object n = record.get(column);

        if (n == null)
            return null;

        String s = n.toString().trim().toLowerCase();

        return calculateStdVisidbDsPdAlexaRelatedlinksCount(s);
    }

    public static Integer calculateStdVisidbDsPdAlexaRelatedlinksCount(String alexaRelatedLinks) {
        if (StringUtils.isEmpty(alexaRelatedLinks))
            return null;

        return StringUtils.countMatches(alexaRelatedLinks, ",") + 1;
    }

    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL);
        metadata.setCategory(Category.ONLINE_PRESENCE);
        metadata.setFundamentalType(FundamentalType.NUMERIC);
        metadata.setStatisticalType(StatisticalType.RATIO);
        metadata.setTags(Tag.EXTERNAL_TRANSFORM);
        metadata.setDisplayDiscretizationStrategy("{\"geometric\": { \"minValue\":1,\"multiplierList\":[2,2.5,2],\"minSamples\":100," //
                + "\"minFreq\":0.01,\"maxBuckets\":5,\"maxPercentile\":1}}");
        return metadata;
    }
}
