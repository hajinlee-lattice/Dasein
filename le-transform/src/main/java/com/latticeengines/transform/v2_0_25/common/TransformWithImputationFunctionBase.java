package com.latticeengines.transform.v2_0_25.common;

public abstract class TransformWithImputationFunctionBase extends TransformFunctionBase {

    private double imputation;

    public TransformWithImputationFunctionBase(Object imputation) {
        setImputation(imputation);
    }

    public void setImputation(Object imputation) {
        this.imputation = ((Number) imputation).doubleValue();
    }

    public double getImputation() {
        return imputation;
    }

}
