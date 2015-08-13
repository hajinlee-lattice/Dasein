package com.latticeengines.domain.exposed.sparkdb;

import java.io.Serializable;

import com.latticeengines.domain.exposed.metadata.Attribute;

public class FunctionExpression implements Serializable {

    private static final long serialVersionUID = 602459174530312057L;

    private Attribute[] sourceAttributes;
    private Attribute targetAttribute;
    private Function function;
    private boolean isNew = true;
    
    public FunctionExpression(String functionName, boolean isNew, Attribute targetAttribute, Attribute... sourceAttributes) {
        try {
            Class<?> functionClass = Class.forName(functionName);
            this.setFunction((Function) functionClass.newInstance());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.isNew = isNew;
        this.sourceAttributes = sourceAttributes;
        this.setTargetAttribute(targetAttribute);
        
    }

    public Attribute[] getSourceAttributes() {
        return sourceAttributes;
    }
    public void setSourceAttributes(Attribute[] sourceAttributes) {
        this.sourceAttributes = sourceAttributes;
    }

    public Attribute getTargetAttribute() {
        return targetAttribute;
    }

    public void setTargetAttribute(Attribute targetAttribute) {
        this.targetAttribute = targetAttribute;
    }

    public Function getFunction() {
        return function;
    }

    public void setFunction(Function function) {
        this.function = function;
    }

    public boolean isNew() {
        return isNew;
    }

}
