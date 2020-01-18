package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.ApprovedUsage;
import com.latticeengines.transform.exposed.metadata.Category;
import com.latticeengines.transform.exposed.metadata.FundamentalType;
import com.latticeengines.transform.exposed.metadata.StatisticalType;
import com.latticeengines.transform.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;

public class StdVisidbDsSpamindicator implements RealTimeTransform {

    private static final Logger log = LoggerFactory.getLogger(StdVisidbDsSpamindicator.class);

    private static final long serialVersionUID = 2222617772602907048L;

    public StdVisidbDsSpamindicator() {
    }

    private static Pattern pattern = Pattern.compile("(^|\\s+)[\\[]*(none|no|not|delete|"
            + "asd|sdf|unknown|undisclosed|" + "null|dont|don\'t|n/a|n.a" + "|abc|xyz|noname|nocompany)($|\\s+)");

    public StdVisidbDsSpamindicator(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column1 = (String) arguments.get("column1");
        String column2 = (String) arguments.get("column2");
        String column3 = (String) arguments.get("column3");
        String column4 = (String) arguments.get("column4");
        String column5 = (String) arguments.get("column5");

        String firstName = column1 == null ? null : String.valueOf(record.get(column1));
        String lastName = column2 == null ? null : String.valueOf(record.get(column2));
        String title = column3 == null ? null : String.valueOf(record.get(column3));
        String phone = column4 == null ? null : String.valueOf(record.get(column4));
        String companyName = column5 == null ? null : String.valueOf(record.get(column5));

        if (firstName.equals("null"))
            firstName = "";
        if (lastName.equals("null"))
            lastName = "";
        if (title.equals("null"))
            title = "";
        if (phone.equals("null"))
            phone = "";
        if (companyName.equals("null"))
            companyName = "";

        return calculateStdVisidbDsSpamindicator(firstName, lastName, title, phone, companyName);
    }

    public static int calculateStdVisidbDsSpamindicator(String firstName, String lastName, String title, String phone,
            String companyName) {
        if (StdVisidbDsFirstnameSameasLastname.calcualteStdVisidbDsFirstnameSameasLastname(firstName, lastName))
            return 1;

        int score = 0;
        score += ds_company_isunusual(companyName);

        if (StdLength.calculateStdLength(companyName) < 5)
            score += 1;

        Double companyNameEntropy = StdVisidbDsCompanynameEntropy.calculateStdVisidbDsCompanynameEntropy(companyName);

        if (companyNameEntropy != null && companyNameEntropy <= 0.03)
            score += 1;

        if (StdLength.calculateStdLength(title) <= 2)
            score += 1;

        Double phoneEntropy = StdEntropy.calculateStdEntropy(phone);

        if (phoneEntropy != null && phoneEntropy <= 0.03)
            score += 1;

        if (score >= 2)
            return 1;

        return 0;
    }

    public static int ds_company_isunusual(String companyName) {
        if (companyName == null || StringUtils.isEmpty(companyName))
            return 0;

        companyName = companyName.toLowerCase();

        if (Pattern.matches(".*[\"#$%+:<=>?@\\^_`{}~].*", companyName))
            return 1;

        if (pattern.matcher(companyName).find())
            return 1;

        try {
            Float.valueOf(companyName);
            return 1;
        } catch (Exception ignore) {
            // companyName is not a float. good
        }

        return 0;
    }

    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL_MODELINSIGHTS);
        metadata.setCategory(Category.LEAD_INFORMATION);
        metadata.setDisplayName("Spam Lead");
        metadata.setFundamentalType(FundamentalType.BOOLEAN);
        metadata.setStatisticalType(StatisticalType.NOMINAL);
        metadata.setTags(Tag.INTERNAL_TRANSFORM);
        metadata.setDescription("Indicator for spam leads");
        return metadata;
    }
}
