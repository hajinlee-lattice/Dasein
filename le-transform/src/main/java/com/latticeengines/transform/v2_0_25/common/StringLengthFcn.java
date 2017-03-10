package com.latticeengines.transform.v2_0_25.common;

public class StringLengthFcn extends TransformWithImputationFunctionBase {

    private int maxStringLen;

    public StringLengthFcn(Object imputation, int maxStringLen) {
        super(imputation);
        this.maxStringLen = maxStringLen;
    }

    @Override
    public double execute(String s) {
        if (s == null) {
            return getImputation();
        }
        return Math.min(s.length(), maxStringLen);
    }

}
