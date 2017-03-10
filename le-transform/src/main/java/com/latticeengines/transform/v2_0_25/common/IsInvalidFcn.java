package com.latticeengines.transform.v2_0_25.common;

public class IsInvalidFcn extends TransformWithImpAndVetoFunctionBase {

    public IsInvalidFcn(Object imputation, String unusualCharSet, String vetoStringSet) {
        super(imputation, unusualCharSet, vetoStringSet);
    }

    @Override
    public void setImputation(Object imputation) {
        // imputation may contain either a Double or an Integer
        super.setImputation(((Number) imputation).doubleValue());
    }

    @Override
    public double execute(String s) {
        if (s == null) {
            return getImputation();
        }
        if (!isValid(s)) {
            return 1.0;
        }
        return 0.0;
    }
}
