package com.latticeengines.common.exposed.util;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.exception.AnnotationValidationError;
import com.latticeengines.common.exposed.validator.BeanValidationService;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

public class ValidationUtils {
    private static final String DEFAULT_OBJECT_NAME = "Object";
    private static final int MATCH_FIELD_VALUE_LENGTH_LIMIT = 500;
    // # and : is recommended not to use by dynamo, || is our internal delimiter
    private static final Pattern INVALID_MATCH_FILED_CHAR_PTN = Pattern.compile("(#|:|\\|\\|)");

    /**
     * Helper to check if any input object is {@literal null}
     *
     * @param objects objects to check
     * @throws NullPointerException if any of the input object is {@literal null}
     */
    public static void checkNotNull(Object... objects) {
        Arrays.stream(objects).forEach(Preconditions::checkNotNull);
    }

    /**
     * Validate with {@link ValidationUtils#check(BeanValidationService, Object, String)} with the default
     * object name
     */
    public static void check(@NotNull BeanValidationService service, @NotNull Object obj) {
        check(service, obj, DEFAULT_OBJECT_NAME);
    }

    /**
     * Validate the input object based on annotation on fields
     * @param service validation service
     * @param obj object to be validated
     * @param objectName object name that will be used in the error message
     * @throws NullPointerException if any of the input is {@literal null}
     * @throws IllegalArgumentException if input object has any invalid field
     */
    public static void check(@NotNull BeanValidationService service, @NotNull Object obj, @NotNull String objectName) {
        Preconditions.checkNotNull(service);
        Preconditions.checkNotNull(objectName);
        Preconditions.checkNotNull(obj, String.format("%s should not be null", objectName));

        Set<AnnotationValidationError> errorSet = service.validate(obj);
        if (!errorSet.isEmpty()) {
            List<String> invalidFieldNames = errorSet
                    .stream()
                    .map(AnnotationValidationError::getFieldName)
                    .collect(Collectors.toList());
            String errorMsg = String.format(
                    "Invalid fields in %s: %s", objectName, String.join(",", invalidFieldNames));
            throw new IllegalArgumentException(errorMsg);
        }
    }

    /**
     * Check whether given value from a match field is valid.
     *
     * @param value
     * @return whether it is valid
     */
    public static boolean isValidMatchFieldValue(String value) {
        if (value == null) {
            // null is valid
            return true;
        }

        // contains no invalid char and within length limit
        return !INVALID_MATCH_FILED_CHAR_PTN.matcher(value).find() && value.length() <= MATCH_FIELD_VALUE_LENGTH_LIMIT;
    }
}
