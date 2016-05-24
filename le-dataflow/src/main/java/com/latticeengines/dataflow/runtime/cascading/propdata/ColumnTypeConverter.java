package com.latticeengines.dataflow.runtime.cascading.propdata;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

import cascading.tuple.coerce.Coercions;
import cascading.tuple.coerce.Coercions.Coerce;

public class ColumnTypeConverter {

    private static final String MAX = "MAX";
    private static final String FLOAT = "FLOAT";
    private static final String INT = "INT";
    private static final String LONG = "BIGINT";
    private static final String BOOLEAN = "BIT";
    private static final String STRING = "NVARCHAR";
    private static final String DOUBLE = "DECIMAL";
    private static final int MAX_NVARCHAR_SIZE = 4000;
    private static final String AVRO_STRING = "string";
    private static final String AVRO_DOUBLE = "double";
    private static final String AVRO_BOOLEAN = "boolean";
    private static final String AVRO_FLOAT = "float";
    private static final String AVRO_INT = "int";
    private static final String AVRO_LONG = "long";

    public static Coerce<?> getCoercionFromTypeName(String typeName) {
        Coerce<?> fieldType = Coercions.STRING;
        String normalizedTypeName = typeName.toUpperCase();
        if (normalizedTypeName.startsWith(STRING)) {
            normalizedTypeName = STRING;
        }

        switch (normalizedTypeName) {
        case DOUBLE:
            fieldType = Coercions.DOUBLE_OBJECT;
            break;
        case BOOLEAN:
            fieldType = Coercions.BOOLEAN_OBJECT;
            break;
        case FLOAT:
            fieldType = Coercions.FLOAT_OBJECT;
            break;
        case INT:
            fieldType = Coercions.INTEGER_OBJECT;
            break;
        case LONG:
            fieldType = Coercions.LONG_OBJECT;
            break;
        default:
            break;
        }
        return fieldType;
    }

    public static String getTypeNameFromCoercion(Coerce<?> CoercionType) {
        String typeName = STRING;

        if (CoercionType == Coercions.DOUBLE_OBJECT) {
            typeName = DOUBLE;
        } else if (CoercionType == Coercions.BOOLEAN_OBJECT) {
            typeName = BOOLEAN;
        } else if (CoercionType == Coercions.FLOAT_OBJECT) {
            typeName = FLOAT;
        } else if (CoercionType == Coercions.INTEGER_OBJECT) {
            typeName = INT;
        } else if (CoercionType == Coercions.LONG_OBJECT) {
            typeName = LONG;
        }
        return typeName;
    }

    public static String getAvroTypeNameFromCoercion(Coerce<?> CoercionType) {
        String avroType = AVRO_STRING;

        if (CoercionType == Coercions.DOUBLE_OBJECT) {
            avroType = AVRO_DOUBLE;
        } else if (CoercionType == Coercions.BOOLEAN_OBJECT) {
            avroType = AVRO_BOOLEAN;
        } else if (CoercionType == Coercions.FLOAT_OBJECT) {
            avroType = AVRO_FLOAT;
        } else if (CoercionType == Coercions.INTEGER_OBJECT) {
            avroType = AVRO_INT;
        } else if (CoercionType == Coercions.LONG_OBJECT) {
            avroType = AVRO_LONG;
        }
        return avroType;
    }

    public static boolean isStringField(String typeName) {
        String normalizedTypeName = typeName.toUpperCase();
        if (normalizedTypeName.startsWith(STRING)) {
            return true;
        }
        return false;
    }

    public static int getStringFieldSize(String typeName) {
        String normalizedTypeName = typeName.toUpperCase();
        if (isStringField(normalizedTypeName)) {
            if (normalizedTypeName.equals(STRING)) {
                return 1;
            }

            // extract value specified within brackets: ex. NVARCHAR(10) => 10
            String sizeStr = normalizedTypeName.substring(STRING.length() + 1, normalizedTypeName.length() - 1);
            if (MAX.equals(sizeStr)) {
                return MAX_NVARCHAR_SIZE;
            } else {
                int size = Integer.parseInt(sizeStr);
                return (size >= MAX_NVARCHAR_SIZE) ? MAX_NVARCHAR_SIZE : size;
            }
        } else {
            throw new LedpException(LedpCode.LEDP_25018,
                    new RuntimeException("Call is valid for " + STRING + " type but passed " + typeName));
        }
    }
}
