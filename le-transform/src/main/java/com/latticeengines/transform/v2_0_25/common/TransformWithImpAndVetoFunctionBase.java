package com.latticeengines.transform.v2_0_25.common;

import java.util.regex.Pattern;

public abstract class TransformWithImpAndVetoFunctionBase extends TransformWithImputationFunctionBase {

    private Pattern pattern_unusualCharSet;
    private Pattern pattern_vetoStringSet;

    public TransformWithImpAndVetoFunctionBase(Object imputation, String unusualCharSet, String vetoStringSet) {
        super(imputation);
        this.pattern_unusualCharSet = Pattern.compile(".*?(" + unusualCharSet + ").*?",
                Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
        this.pattern_vetoStringSet = Pattern.compile(".*?(" + vetoStringSet + ").*?",
                Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
    }

    public boolean isValid(String s) {
        try {
            Double.valueOf(s);
            return false;
        } catch (NumberFormatException e) {
        }
        if (pattern_unusualCharSet.matcher(s).matches()) {
            return false;
        }
        if (pattern_vetoStringSet.matcher(s).matches()) {
            return false;
        }
        return true;
    }

}
