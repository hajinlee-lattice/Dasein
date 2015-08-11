package com.latticeengines.common.exposed.validator;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.annotation.Annotation;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.validator.annotation.RequiredKeysInMap;
import com.latticeengines.common.exposed.validator.impl.RequiredKeysInMapAnnotationValidator;

public class RequiredKeysInMapAnnotationValidatorUnitTestNG {
    private static final String MUST_HAVE_KEY_1 = "mustHaveKey1";
    private static final String MAY_HAVE_KEY_2 = "mayHaveKey2";
    private static final String MAY_HAVE_VALUE_1 = "mayHaveValue1";

    private RequiredKeysInMapAnnotationValidator requiredKeysInMapAnnotationValidator;

    @BeforeClass(groups = "unit")
    public void setup() {
        this.requiredKeysInMapAnnotationValidator = new RequiredKeysInMapAnnotationValidator();
    }

    private static class ObjectWithRequiredKeysInMapAnnotations {
        @RequiredKeysInMap(keys = { MUST_HAVE_KEY_1 })
        private List<Map.Entry<String, ?>> attr1;
    }

    @Test(groups = "unit")
    public void objectWithRequiredKeysInMapAnnotations_fieldIsNull_validatorReturnsFalse() throws NoSuchFieldException {
        Annotation requiredKeysInMapAnnotation = ObjectWithRequiredKeysInMapAnnotations.class.getDeclaredField("attr1")
                .getAnnotation(RequiredKeysInMap.class);
        assertFalse(this.requiredKeysInMapAnnotationValidator.validate(null, requiredKeysInMapAnnotation));
    }

    @Test(groups = "unit")
    public void objectWithRequiredKeysInMapAnnotations_fieldContainsRequiredKey_validatorReturnsTrue()
            throws NoSuchFieldException {
        Annotation requiredKeysInMapAnnotation = ObjectWithRequiredKeysInMapAnnotations.class.getDeclaredField("attr1")
                .getAnnotation(RequiredKeysInMap.class);
        Map.Entry<String, String> pair = new AbstractMap.SimpleEntry<String, String>(MUST_HAVE_KEY_1, MAY_HAVE_VALUE_1);
        List<Map.Entry<String, String>> list = new ArrayList<Map.Entry<String, String>>();
        list.add(pair);
        assertTrue(this.requiredKeysInMapAnnotationValidator.validate(list, requiredKeysInMapAnnotation));
    }

    @Test(groups = "unit")
    public void objectWithRequiredKeysInMapAnnotations_fieldContainsRequiredKeyButNullValue_validatorReturnsFalse()
            throws NoSuchFieldException {
        Annotation requiredKeysInMapAnnotation = ObjectWithRequiredKeysInMapAnnotations.class.getDeclaredField("attr1")
                .getAnnotation(RequiredKeysInMap.class);
        Map.Entry<String, String> pair = new AbstractMap.SimpleEntry<String, String>(MUST_HAVE_KEY_1, null);
        List<Map.Entry<String, String>> list = new ArrayList<Map.Entry<String, String>>();
        list.add(pair);
        assertFalse(this.requiredKeysInMapAnnotationValidator.validate(list, requiredKeysInMapAnnotation));
    }

    @Test(groups = "unit")
    public void objectWithRequiredKeysInMapAnnotations_fieldContainsRequiredKeyButEmptyString_validatorReturnsFalse()
            throws NoSuchFieldException {
        Annotation requiredKeysInMapAnnotation = ObjectWithRequiredKeysInMapAnnotations.class.getDeclaredField("attr1")
                .getAnnotation(RequiredKeysInMap.class);
        Map.Entry<String, String> pair = new AbstractMap.SimpleEntry<String, String>(MUST_HAVE_KEY_1, "");
        List<Map.Entry<String, String>> list = new ArrayList<Map.Entry<String, String>>();
        list.add(pair);
        assertFalse(this.requiredKeysInMapAnnotationValidator.validate(list, requiredKeysInMapAnnotation));
    }

    @Test(groups = "unit")
    public void objectWithRequiredKeysInMapAnnotations_fieldNotContainsRequiredKey_validatorReturnsFalse()
            throws NoSuchFieldException {
        Annotation requiredKeysInMapAnnotation = ObjectWithRequiredKeysInMapAnnotations.class.getDeclaredField("attr1")
                .getAnnotation(RequiredKeysInMap.class);
        Map.Entry<String, String> pair = new AbstractMap.SimpleEntry<String, String>(MAY_HAVE_KEY_2, MAY_HAVE_VALUE_1);
        List<Map.Entry<String, String>> list = new ArrayList<Map.Entry<String, String>>();
        list.add(pair);
        assertFalse(this.requiredKeysInMapAnnotationValidator.validate(list, requiredKeysInMapAnnotation));
    }

    @Test(groups = "unit")
    public void objectWithRequiredKeysInMapAnnotations_fieldContainsRequiredKeyAndOtherPairs_validatorReturnsTrue()
            throws NoSuchFieldException {
        Annotation requiredKeysInMapAnnotation = ObjectWithRequiredKeysInMapAnnotations.class.getDeclaredField("attr1")
                .getAnnotation(RequiredKeysInMap.class);
        Map.Entry<String, String> pair1 = new AbstractMap.SimpleEntry<String, String>(MAY_HAVE_KEY_2, null);
        Map.Entry<String, String> pair2 = new AbstractMap.SimpleEntry<String, String>(MUST_HAVE_KEY_1, MAY_HAVE_VALUE_1);
        List<Map.Entry<String, String>> list = new ArrayList<Map.Entry<String, String>>();
        list.add(pair1);
        list.add(pair2);
        assertTrue(this.requiredKeysInMapAnnotationValidator.validate(list, requiredKeysInMapAnnotation));
    }

}
