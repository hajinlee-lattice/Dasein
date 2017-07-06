package com.latticeengines.common.exposed.validator;

import static org.testng.Assert.assertTrue;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.exception.AnnotationValidationError;
import com.latticeengines.common.exposed.validator.annotation.AllowedValues;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.validator.annotation.RequiredKeysInMap;
import com.latticeengines.common.exposed.validator.impl.BeanValidationServiceImpl;

public class BeanValidationServiceImplUnitTestNG {

    private static final String ALLOWED_VALUE_1 = "ALLOWED_VALUE_1";
    private static final String ALLOWED_VALUE_2 = "ALLOWED_VALUE_2";
    private static final String NOT_ALLOWED_VALUE = "NOT_ALLOWED_VALUE";
    private static final String MUST_HAVE_KEY_1 = "mustHaveKey1";
    private static final String MAY_HAVE_KEY_2 = "mayHaveKey2";
    private static final String MAY_HAVE_VALUE_1 = "mayHaveValue1";
    private static final String ALLOWED_VALUE_ANNOTATION_VALIDATOR = "AllowedValuesAnnotationValidator";
    private static final String NOT_NULL_ANNOTATION_VALIDATOR = "NotNullAnnotationValidator";
    private static final String REQUIRED_KEYS_IN_MAP_ANNOTATION_VALIDATOR = "RequiredKeysInMapAnnotationValidator";
    private static final String NOT_EMPTY_STRING_VALIDATOR = "NotEmptyStringAnnotationValidator";
    private static final String NOT_EMPTY_STRING = "notEmptyString";
    private static final Integer ONE = 1;

    private BeanValidationServiceImpl beanValidationServiceImpl;
    private ObjectWithAnnotations objectWithAnnotations;
    private List<String> allowedList;
    private List<String> notAllowedList;
    private Map.Entry<String, String> validPair;
    private Map.Entry<String, String> invalidPair;
    private Set<AnnotationValidationError> result;

    private static class ObjectWithAnnotationsBase {
        @AllowedValues(values = { ALLOWED_VALUE_1, ALLOWED_VALUE_2 })
        protected String attr1;

        @SuppressWarnings("unused")
        protected String attr2;
    }

    private static class ObjectWithAnnotations extends ObjectWithAnnotationsBase {
        @AllowedValues(values = { ALLOWED_VALUE_1 })
        @NotNull
        private List<String> attr3;

        @NotNull
        private Integer attr4;

        @RequiredKeysInMap(keys = { MUST_HAVE_KEY_1 })
        private List<Map.Entry<String, String>> attr5;

        @NotEmptyString
        private String attr6;

        public void setAttr1(String attr) {
            this.attr1 = attr;
        }

        public void setAttr3(List<String> attr) {
            this.attr3 = attr;
        }

        public void setAttr4(Integer attr) {
            this.attr4 = attr;
        }

        public void setAttr5(List<Map.Entry<String, String>> attr) {
            this.attr5 = attr;
        }

        public void setAttr6(String attr) {
            this.attr6 = attr;
        }
    }

    @BeforeClass(groups = "unit")
    public void setup() {
        this.beanValidationServiceImpl = new BeanValidationServiceImpl();
        this.objectWithAnnotations = new ObjectWithAnnotations();
        this.allowedList = Arrays.asList(ALLOWED_VALUE_1);
        this.notAllowedList = Arrays.asList(NOT_ALLOWED_VALUE);
        this.validPair = new AbstractMap.SimpleEntry<String, String>(MUST_HAVE_KEY_1, MAY_HAVE_VALUE_1);
        this.invalidPair = new AbstractMap.SimpleEntry<String, String>(MAY_HAVE_KEY_2, MAY_HAVE_VALUE_1);
    }

    @Test(groups = "unit")
    public void objectWithAnnotations_baseClassAttributeDoesNotMatchAnnotation_validationServiceReturnsError() {
        AnnotationValidationError error = new AnnotationValidationError("attr1", ALLOWED_VALUE_ANNOTATION_VALIDATOR);
        this.objectWithAnnotations.setAttr1(NOT_ALLOWED_VALUE);
        this.objectWithAnnotations.setAttr3(allowedList);
        this.objectWithAnnotations.setAttr4(ONE);
        List<Map.Entry<String, String>> list = new ArrayList<Map.Entry<String, String>>();
        list.add(validPair);
        this.objectWithAnnotations.setAttr5(list);
        this.objectWithAnnotations.setAttr6(NOT_EMPTY_STRING);
        result = this.beanValidationServiceImpl.validate(this.objectWithAnnotations);
        assertTrue(result.size() == 1);
        assertTrue(result.contains(error));
    }

    @Test(groups = "unit")
    public void objectWithAnnotations_derivedClassAttributeDoesNotMatchAllowedValuesAnnotation_validationServiceReturnsError() {
        AnnotationValidationError error = new AnnotationValidationError("attr3", ALLOWED_VALUE_ANNOTATION_VALIDATOR);
        this.objectWithAnnotations.setAttr1(ALLOWED_VALUE_1);
        this.objectWithAnnotations.setAttr3(notAllowedList);
        this.objectWithAnnotations.setAttr4(ONE);
        List<Map.Entry<String, String>> list = new ArrayList<Map.Entry<String, String>>();
        list.add(validPair);
        this.objectWithAnnotations.setAttr5(list);
        this.objectWithAnnotations.setAttr6(NOT_EMPTY_STRING);
        result = this.beanValidationServiceImpl.validate(this.objectWithAnnotations);
        assertTrue(result.size() == 1);
        assertTrue(result.contains(error));
    }

    @Test(groups = "unit")
    public void objectWithAnnotations_derviedClassAttributeDoesNotMatchNotNullAnnotation_validationServiceReturnsError() {
        AnnotationValidationError error = new AnnotationValidationError("attr4", NOT_NULL_ANNOTATION_VALIDATOR);
        this.objectWithAnnotations.setAttr1(ALLOWED_VALUE_1);
        this.objectWithAnnotations.setAttr3(allowedList);
        this.objectWithAnnotations.setAttr4(null);
        List<Map.Entry<String, String>> list = new ArrayList<Map.Entry<String, String>>();
        list.add(validPair);
        this.objectWithAnnotations.setAttr5(list);
        this.objectWithAnnotations.setAttr6(NOT_EMPTY_STRING);
        result = this.beanValidationServiceImpl.validate(this.objectWithAnnotations);
        assertTrue(result.size() == 1);
        assertTrue(result.contains(error));
    }

    @Test(groups = "unit")
    public void objectWithAnnotations_attributeWithDoubleNotationIsSetToNull_validationServiceReturnsError() {
        AnnotationValidationError error = new AnnotationValidationError("attr3", NOT_NULL_ANNOTATION_VALIDATOR);
        this.objectWithAnnotations.setAttr1(ALLOWED_VALUE_2);
        this.objectWithAnnotations.setAttr3(null);
        this.objectWithAnnotations.setAttr4(ONE);
        List<Map.Entry<String, String>> list = new ArrayList<Map.Entry<String, String>>();
        list.add(validPair);
        this.objectWithAnnotations.setAttr5(list);
        this.objectWithAnnotations.setAttr6(NOT_EMPTY_STRING);
        result = this.beanValidationServiceImpl.validate(this.objectWithAnnotations);
        assertTrue(result.size() == 1);
        assertTrue(result.contains(error));
    }

    @Test(groups = "unit")
    public void objectWithAnnotations_derviedClassAttributeDoesNotMatchRequiredKeysInMapAnnotation_validationServiceReturnsError() {
        AnnotationValidationError error = new AnnotationValidationError("attr5",
                REQUIRED_KEYS_IN_MAP_ANNOTATION_VALIDATOR);
        this.objectWithAnnotations.setAttr1(ALLOWED_VALUE_1);
        this.objectWithAnnotations.setAttr3(allowedList);
        this.objectWithAnnotations.setAttr4(ONE);
        List<Map.Entry<String, String>> list = new ArrayList<Map.Entry<String, String>>();
        list.add(invalidPair);
        this.objectWithAnnotations.setAttr5(list);
        this.objectWithAnnotations.setAttr6(NOT_EMPTY_STRING);
        result = this.beanValidationServiceImpl.validate(this.objectWithAnnotations);
        assertTrue(result.size() == 1);
        assertTrue(result.contains(error));
    }

    @Test(groups = "unit")
    public void objectWithAnnotations_derviedClassAttributeDoesNotMatchNotEmptyStringAnnotation_validationServiceReturnsError() {
        AnnotationValidationError error = new AnnotationValidationError("attr6", NOT_EMPTY_STRING_VALIDATOR);
        this.objectWithAnnotations.setAttr1(ALLOWED_VALUE_1);
        this.objectWithAnnotations.setAttr3(allowedList);
        this.objectWithAnnotations.setAttr4(ONE);
        List<Map.Entry<String, String>> list = new ArrayList<Map.Entry<String, String>>();
        list.add(validPair);
        this.objectWithAnnotations.setAttr5(list);
        this.objectWithAnnotations.setAttr6("");
        result = this.beanValidationServiceImpl.validate(this.objectWithAnnotations);
        assertTrue(result.size() == 1);
        assertTrue(result.contains(error));
    }

    @Test(groups = "unit")
    public void objectWithAnnotations_attributesWithMultipleAnnotationViolations_validationServiceReturnsErrors() {
        AnnotationValidationError error1 = new AnnotationValidationError("attr1", ALLOWED_VALUE_ANNOTATION_VALIDATOR);
        AnnotationValidationError error2 = new AnnotationValidationError("attr3", NOT_NULL_ANNOTATION_VALIDATOR);
        AnnotationValidationError error3 = new AnnotationValidationError("attr5",
                REQUIRED_KEYS_IN_MAP_ANNOTATION_VALIDATOR);
        this.objectWithAnnotations.setAttr1(NOT_ALLOWED_VALUE);
        this.objectWithAnnotations.setAttr3(null);
        this.objectWithAnnotations.setAttr4(ONE);
        List<Map.Entry<String, String>> list = new ArrayList<Map.Entry<String, String>>();
        list.add(invalidPair);
        this.objectWithAnnotations.setAttr5(list);
        this.objectWithAnnotations.setAttr6(NOT_EMPTY_STRING);
        result = this.beanValidationServiceImpl.validate(this.objectWithAnnotations);
        assertTrue(result.size() == 3);
        assertTrue(result.contains(error1));
        assertTrue(result.contains(error2));
        assertTrue(result.contains(error3));
    }

    @Test(groups = "unit")
    public void objectWithAnnotations_objectMatchesAllAnnotations_validationServiceReturnsEmptyErrorSet() {
        this.objectWithAnnotations.setAttr1(ALLOWED_VALUE_1);
        this.objectWithAnnotations.setAttr3(allowedList);
        this.objectWithAnnotations.setAttr4(ONE);
        List<Map.Entry<String, String>> list = new ArrayList<Map.Entry<String, String>>();
        list.add(validPair);
        this.objectWithAnnotations.setAttr5(list);
        this.objectWithAnnotations.setAttr6(NOT_EMPTY_STRING);
        result = this.beanValidationServiceImpl.validate(this.objectWithAnnotations);
        assertTrue(result.size() == 0);
    }

}
