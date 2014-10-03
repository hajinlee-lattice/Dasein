package com.latticeengines.domain.exposed.sparkdb;

import com.latticeengines.domain.exposed.eai.Attribute;

public class FunctionExpression {

    private Attribute attributeToActOn;
    private Attribute attributeToCreate;
    private Function<?> function;
    private boolean isNew = true;
    
    public FunctionExpression(Attribute attributeToActOn, Function<?> function) {
        this(attributeToActOn, null, function);
        isNew = false;
    }
    
    public FunctionExpression(Attribute attributeToActOn, Attribute attributeToCreate, Function<?> function) {
        this.setAttributeToActOn(attributeToActOn);
        this.setFunction(function);
    }

    public Attribute getAttributeToActOn() {
        return attributeToActOn;
    }
    public void setAttributeToActOn(Attribute attributToActOn) {
        this.attributeToActOn = attributToActOn;
    }

    public Attribute getAttributeToCreate() {
        return attributeToCreate;
    }

    public void setAttributeToCreate(Attribute attributeToCreate) {
        this.attributeToCreate = attributeToCreate;
    }

    public Function<?> getFunction() {
        return function;
    }

    public void setFunction(Function<?> function) {
        this.function = function;
    }

    public boolean isNew() {
        return isNew;
    }

}
