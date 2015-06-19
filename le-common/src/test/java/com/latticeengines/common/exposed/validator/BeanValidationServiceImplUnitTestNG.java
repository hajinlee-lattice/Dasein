package com.latticeengines.common.exposed.validator;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.expection.BeanValidationException;
import com.latticeengines.common.exposed.validator.annotation.AllowedValues;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.validator.impl.BeanValidationServiceImpl;

public class BeanValidationServiceImplUnitTestNG {
    private static final String ALLOWED_VALUE_1 = "ALLOWED_VALUE_1";
    private static final String ALLOWED_VALUE_2 = "ALLOWED_VALUE_2";
    private static final String NOT_ALLOWED_VALUE = "NOT_ALLOWED_VALUE";
    private static final Integer ONE = 1;

    private BeanValidationServiceImpl beanValidationServiceImpl;
    private ObjectWithAnnotations objectWithAnnotations;

    private static class ObjectWithAnnotationsBase {
        @AllowedValues(values = { ALLOWED_VALUE_1, ALLOWED_VALUE_2 })
        protected String attr1;

        @SuppressWarnings("unused")
        protected String attr2;
    }

    private static class ObjectWithAnnotations extends ObjectWithAnnotationsBase {
        @AllowedValues(values = { ALLOWED_VALUE_1 })
        @NotNull
        private String attr3;

        @NotNull
        private Integer attr4;

        public void setAttr1(String attr) {
            this.attr1 = attr;
        }

        public void setAttr3(String attr) {
            this.attr3 = attr;
        }

        public void setAttr4(Integer attr) {
            this.attr4 = attr;
        }
    }

    @BeforeClass(groups = "unit")
    public void setup() {
        this.beanValidationServiceImpl = new BeanValidationServiceImpl();
        this.objectWithAnnotations = new ObjectWithAnnotations();
    }

    @Test(groups = "unit", expectedExceptions = BeanValidationException.class)
    public void objectWithAnnotations_baseClassAttributeDoesNotMatchAnnotation_validationServiceReturnsFalse() {
        this.objectWithAnnotations.setAttr1(NOT_ALLOWED_VALUE);
        this.objectWithAnnotations.setAttr3(ALLOWED_VALUE_1);
        this.objectWithAnnotations.setAttr4(ONE);
        this.beanValidationServiceImpl.validate(this.objectWithAnnotations);
    }

    @Test(groups = "unit", expectedExceptions = BeanValidationException.class)
    public void objectWithAnnotations_derivedClassAttributeDoesNotMatchAllowedValuesAnnotation_validationServiceReturnsFalse() {
        this.objectWithAnnotations.setAttr1(ALLOWED_VALUE_1);
        this.objectWithAnnotations.setAttr3(NOT_ALLOWED_VALUE);
        this.objectWithAnnotations.setAttr4(ONE);
        this.beanValidationServiceImpl.validate(this.objectWithAnnotations);
    }

    @Test(groups = "unit", expectedExceptions = BeanValidationException.class)
    public void objectWithAnnotations_derviedClassAttributeDoesNotMatchNotNullAnnotation_validationServiceReturnsFalse() {
        this.objectWithAnnotations.setAttr1(ALLOWED_VALUE_1);
        this.objectWithAnnotations.setAttr3(ALLOWED_VALUE_1);
        this.objectWithAnnotations.setAttr4(null);
        this.beanValidationServiceImpl.validate(this.objectWithAnnotations);
    }

    @Test(groups = "unit", expectedExceptions = BeanValidationException.class)
    public void objectWithAnnotations_attributeWithDoubleNotationIsSetToNull_validationServiceReturnsFalse() {
        this.objectWithAnnotations.setAttr1(ALLOWED_VALUE_2);
        this.objectWithAnnotations.setAttr3(null);
        this.objectWithAnnotations.setAttr4(ONE);
        this.beanValidationServiceImpl.validate(this.objectWithAnnotations);
    }

    @Test(groups = "unit")
    public void objectWithAnnotations_objectMatchesAllAnnotations_validationServiceReturnsTrue() {
        this.objectWithAnnotations.setAttr1(ALLOWED_VALUE_1);
        this.objectWithAnnotations.setAttr3(ALLOWED_VALUE_1);
        this.objectWithAnnotations.setAttr4(ONE);
        this.beanValidationServiceImpl.validate(this.objectWithAnnotations);
    }
}
