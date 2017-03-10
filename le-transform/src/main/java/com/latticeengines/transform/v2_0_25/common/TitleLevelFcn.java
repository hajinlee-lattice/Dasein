package com.latticeengines.transform.v2_0_25.common;

import java.util.regex.Pattern;

public class TitleLevelFcn extends TransformWithImputationFunctionBase {

    private Pattern patternSenior;
    private Pattern patternManager;
    private Pattern patternDirector;
    private Pattern patternVP;

    public TitleLevelFcn(Object imputation, String regexSenior, String regexManager, String regexDirector,
            String regexVP) {
        super(imputation);
        this.patternSenior = Pattern.compile(".*?(" + regexSenior + ").*?",
                Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
        this.patternManager = Pattern.compile(".*?(" + regexManager + ").*?",
                Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
        this.patternDirector = Pattern.compile(".*?(" + regexDirector + ").*?",
                Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
        this.patternVP = Pattern.compile(".*?(" + regexVP + ").*?", Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
    }

    @Override
    public double execute(String s) {
        if (s == null) {
            return getImputation();
        }
        double isVPAbove = patternVP.matcher(s).matches() ? 1.0 : 0.0;
        double isDirector = patternDirector.matcher(s).matches() ? 1.0 : 0.0;
        double isManager = patternManager.matcher(s).matches() ? 1.0 : 0.0;
        double isSenior = patternSenior.matcher(s).matches() ? 1.0 : 0.0;
        return (isVPAbove * 8.0 + isDirector * 4.0 + isManager * 2.0 + isSenior);
    }

}
