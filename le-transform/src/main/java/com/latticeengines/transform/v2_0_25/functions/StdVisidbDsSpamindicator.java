package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;

public class StdVisidbDsSpamindicator implements RealTimeTransform {

    public StdVisidbDsSpamindicator(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments,
            Map<String, Object> record) {
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

        if(firstName.equals("null") || lastName.equals("null") || title.equals("null") ||
                phone.equals("null") || companyName.equals("null"))
            return null;

        return calculateStdVisidbDsSpamindicator(firstName, lastName, title,
                phone, companyName);
    }

    public static int calculateStdVisidbDsSpamindicator(String firstName,
            String lastName, String title, String phone, String companyName) {
        if (StdVisidbDsFirstnameSameasLastname
                .calcualteStdVisidbDsFirstnameSameasLastname(firstName,
                        lastName))
            return 1;

        int score = 0;
        score += ds_company_isunusual(companyName);

        if (StdLength.calculateStdLength(companyName) < 5)
            score += 1;

        Double companyNameEntropy = StdVisidbDsCompanynameEntropy
                .calculateStdVisidbDsCompanynameEntropy(companyName);

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

        if (Pattern.matches(".*[\"#$%+:<=>?@\\^_`{}~].*", companyName) == true)
            return 1;

        // (.*?\\b)no(\\b.*?) - this will match a "no" by itself
        if (Pattern
                .matches(
                        "(^|\\s+)[\\[]*(none|(.*?\\b)no(\\b.*?)|(.*?\\b)not(\\b.*?)|(.*?\\b)delete(\\b.*?)|"
                                + "(.*?\\b)asd(\\b.*?)|(.*?\\b)sdf(\\b.*?)|(.*?\\b)unknown(\\b.*?)|(.*?\\b)undisclosed(\\b.*?)|"
                                + "(.*?\\b)null(\\b.*?)|(.*?\\b)dont(\\b.*?)|(.*?\\b)don\'t(\\b.*?)|(.*?\\b)n/a(\\b.*?)|(.*?\\b)n\\.a(\\b.*?)"
                                + "|(.*?\\b)abc(?!\\w)|(.*?\\b)xyz(\\b.*?)|(.*?\\b)noname(\\b.*?)|(.*?\\b)nocompany(\\b.*?))",
                        companyName))
            return 1;

        try {
            Float f = Float.valueOf(companyName);
            return 1;
        } catch (Exception e) {
            // pass
        }

        return 0;
    }
}
