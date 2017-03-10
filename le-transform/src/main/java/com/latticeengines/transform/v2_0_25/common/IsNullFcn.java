package com.latticeengines.transform.v2_0_25.common;

public class IsNullFcn extends TransformFunctionBase {

    public IsNullFcn() {
    }

    @Override
    public double execute(String s) {
        if (s == null) {
            return 1.0;
        }
        return 0.0;
    }

}
