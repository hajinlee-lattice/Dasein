package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;
import java.util.regex.Pattern;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.ApprovedUsage;
import com.latticeengines.transform.exposed.metadata.Category;
import com.latticeengines.transform.exposed.metadata.FundamentalType;
import com.latticeengines.transform.exposed.metadata.StatisticalType;
import com.latticeengines.transform.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;

public class StdVisidbDsTitleLevel implements RealTimeTransform {

    private static final long serialVersionUID = 5331340048981069485L;

    public StdVisidbDsTitleLevel() {
    }

    public StdVisidbDsTitleLevel(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object n = record.get(column);

        if (n == null)
            return 0;

        String s = n.toString().toLowerCase();

        return calculateStdVisidbDsTitleLevel(s);
    }

    public static int calculateStdVisidbDsTitleLevel(String s) {
        return StdVisidbDsTitleLevel.dsTitleIssenior(s)
                + (2 * StdVisidbDsTitleLevel.dsTitleIsmanager(s) + (4 * StdVisidbDsTitleLevel.dsTitleIsdirector(s) + 8 * StdVisidbDsTitleLevel
                        .dsTitleIsvpabove(s)));
    }

    private static int dsTitleIssenior(String s) {
        return Pattern.matches("(.*?\\b)sr(\\b.*?)|(.*?\\b)senior(\\b.*?)", s) ? 1 : 0;
    }

    private static int dsTitleIsmanager(String s) {
        return Pattern.matches("(.*?\\b)mgr(.*)|(.*)manag(.*)", s) ? 1 : 0;
    }

    private static int dsTitleIsdirector(String s) {
        return Pattern.matches("(.*?\\b)dir(.*)", s) ? 1 : 0;
    }

    private static int dsTitleIsvpabove(String s) {
        return Pattern.matches(
                "(.*?\\b)vp(\\b.*?)|(.*?\\b)pres(.*)|(.*?\\b)chief(\\b.*?)|(.*)(?<!\\w)c[tefmxdosi]o(?!\\w)(.*)", s) ? 1
                : 0;
    }

    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL_MODELINSIGHTS);
        metadata.setCategory(Category.LEAD_INFORMATION);
        metadata.setFundamentalType(FundamentalType.NUMERIC);
        metadata.setStatisticalType(StatisticalType.ORDINAL);
        metadata.setTags(Tag.INTERNAL_TRANSFORM);
        metadata.setDisplayDiscretizationStrategy("{\"linear\": { \"minValue\":0,\"stepSize\":1,\"minSamples\":100," //
                + "\"minFreq\":0.01,\"maxBuckets\":5,\"maxPercentile\":1}}");
        metadata.setDescription("Numeric score corresponding to Job Title level; Senior Titles have higher values");
        metadata.setDisplayName("Job Title Seniority");
        return metadata;
    }
}
