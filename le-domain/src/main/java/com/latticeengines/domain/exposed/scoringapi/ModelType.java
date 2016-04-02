package com.latticeengines.domain.exposed.scoringapi;


public enum ModelType {

    ACCOUNT, //
    CONTACT;

    @SuppressWarnings("unchecked")
    public static <T extends Enum<T>> T getEnumFromString(Class<T> enumClass, String value) {
        for (Enum<?> enumValue : enumClass.getEnumConstants()) {
            if (enumValue.toString().equalsIgnoreCase(value)) {
                return (T) enumValue;
            }
        }

        // Construct an error message that indicates all possible values for the enum.
        StringBuilder errorMessage = new StringBuilder();
        boolean first = true;
        for (Enum<?> enumValue : enumClass.getEnumConstants()) {
            errorMessage.append(first ? "" : ", ").append(enumValue);
            first = false;
        }
        throw new IllegalArgumentException(value + " is an invalid value. Supported values are " + errorMessage);
    }

}
