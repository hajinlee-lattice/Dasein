package com.latticeengines.transform.v2_0_25.common;

import java.util.regex.Pattern;

public class IsRegexMatchFcn extends TransformWithImputationFunctionBase {

    private Pattern pattern;

    public IsRegexMatchFcn(Object imputation, String regex) {
        super(imputation);
        regex = ".*?(" + regex + ").*?";
        this.pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
    }

    @Override
    public double execute(String s) {
        if (s == null) {
            return getImputation();
        }
        if (pattern.matcher(s).matches()) {
            return 1.0;
        }
        return 0.0;
    }

}
