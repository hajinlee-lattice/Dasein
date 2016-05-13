package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.ApprovedUsage;
import com.latticeengines.transform.exposed.metadata.Category;
import com.latticeengines.transform.exposed.metadata.FundamentalType;
import com.latticeengines.transform.exposed.metadata.StatisticalType;
import com.latticeengines.transform.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;

public class StdVisidbDsIndustryGroup implements RealTimeTransform {

    private static final long serialVersionUID = -5157087545940247272L;

    public StdVisidbDsIndustryGroup() {
    }

    public StdVisidbDsIndustryGroup(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        String s = column == null ? null : String.valueOf(record.get(column));

        if (s.equals("null"))
            return null;

        return calculateStdVisidbDsIndustryGroup(s);
    }

    public static String calculateStdVisidbDsIndustryGroup(String industryGroup) {
        if (StringUtils.isEmpty(industryGroup))
            return null;

        industryGroup = industryGroup.trim().toLowerCase();

        if (Pattern.matches("(.*?\\b)credit(.*?\\b)|(.*?\\b)financial(.*?\\b)|(.*?\\b)bank(.*?\\b)", industryGroup))
            return "Finance";
        else if (Pattern.matches("(.*)tech(.*)|(.*?\\b)tele(.*?\\b)", industryGroup))
            return "Tech";
        else if (Pattern.matches("(.*?\\b)health(.*?\\b)|(.*?\\b)medical(.*?\\b)|(.*?\\b)pharm(.*?\\b)", industryGroup))
            return "Health Care";
        else if (Pattern.matches("(.*?\\b)real(.*?\\b)|(.*?\\b)property(.*?\\b)", industryGroup))
            return "Real Estate/Property Mgmt";
        else if (Pattern.matches("(.*?\\b)staff(.*?\\b)|(.*?\\b)hr(.*?\\b)", industryGroup))
            return "HR/Staffing";
        else if (Pattern.matches("(.*?\\b)services(.*?\\b)|(.*?\\b)consulting(.*?\\b)", industryGroup))
            return "Business Service";
        else if (Pattern.matches("(.*?\\b)education(.*?\\b)", industryGroup))
            return "Education";
        else if (Pattern.matches("(.*?\\b)equipment(.*?\\b)", industryGroup))
            return "Equipment";
        else if (Pattern.matches("(.*?\\b)util(.*?\\b)", industryGroup))
            return "Utilities";
        else if (Pattern.matches("(.*?\\b)retail(.*?\\b)", industryGroup))
            return "Retail";
        else if (Pattern.matches("(.*?\\b)transport(.*?\\b)", industryGroup))
            return "Transportation";
        else if (Pattern.matches("(.*?\\b)account(.*?\\b)|(.*?\\b)legal(.*?\\b)", industryGroup))
            return "Accounting/Legal";
        else if (Pattern.matches("(.*?\\b)construction(.*?\\b)", industryGroup))
            return "Construction";
        else if (Pattern.matches("(.*?\\b)profit(.*?\\b)", industryGroup))
            return "Non-Profit";
        else if (Pattern.matches("(.*?\\b)insurance(.*?\\b)", industryGroup))
            return "Insurance";
        else if (Pattern.matches("(.*?\\b)manufact(.*?\\b)", industryGroup))
            return "Manufacturing";

        return "Other";
    }

    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        metadata.setCategory(Category.LEAD_INFORMATION);
        metadata.setFundamentalType(FundamentalType.ALPHA);
        metadata.setStatisticalType(StatisticalType.NOMINAL);
        metadata.setTags(Tag.INTERNAL_TRANSFORM);
        metadata.setDescription("Rollup of Industry field from Marketing Automation");
        metadata.setDisplayName("Industry Rollup");
        return metadata;
    }
}
