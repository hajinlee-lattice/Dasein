package com.latticeengines.transform.v2_0_25.functions;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.Map;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.transform.exposed.RealTimeTransform;

/* This class re-implements the Python2 function encoder.py which is defined in the following places
 * 1. /le-scoring/src/test/resources/com/latticeengines/scoring/models/supportedFiles/encoder.py
 * 2. /le-dataplatform/src/main/python/pipeline/encoder.py
 */

public class Encoder implements RealTimeTransform {

    private static final long serialVersionUID = 9104503413595249740L;

    public Encoder() {}

    public Encoder(String modelPath) {
    }

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object value = record.get(column);

        String valueAsString;
        if (value != null) {
            valueAsString = value.toString();
        } else {
            valueAsString = "NULL";
        }

        if (valueAsString == null //
                || valueAsString.equals("None") //
                || valueAsString.equals("null") //
                || valueAsString.equals("NULL")) {
            valueAsString = "NULL";
            return "1463472903";
        }

        if (value.getClass() == Boolean.class) {
            if (valueAsString.toString().equals("0")) {
                return "0";
            } else if (valueAsString.toString().equals("1")) {
                return "1";
            } else if (valueAsString.toString().toLowerCase().equals("true")) {
                return "1";
            } else if (valueAsString.toString().toLowerCase().equals("false")) {
                return "0";
            }
        }

        if (value.getClass() == Integer.class //
                || value.getClass() == Long.class //
                || value.getClass() == Float.class || value.getClass() == Double.class) {
            if (valueAsString.matches("^-?\\d+$") //
                    || valueAsString.matches("^-?\\d+.\\d+$") //
                    || valueAsString.equals("false") //
                    || valueAsString.equals("true")) {
                try {
                    Double d = Double.parseDouble(valueAsString);
                    if (d.isNaN()) {
                        valueAsString = "NULL";
                    } else {
                        return valueAsString;
                    }
                } catch (Exception e) {
                    valueAsString = "NULL";
                }
            }
        }

        BigInteger code = BigInteger.ZERO;
        byte[] bytes;
        try {
            bytes = valueAsString.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            // Use default encoding
            bytes = valueAsString.getBytes();
        }
        for (int i = 0; i < valueAsString.getBytes().length; i++) {
            code = BigInteger.valueOf((bytes[i] & 0xffl)).add(code.shiftLeft(6)).add(code.shiftLeft(16)).subtract(code);
        }
        code = code.and(BigInteger.valueOf(4294967295l));

        return code;
    }

    @Override
    public Attribute getMetadata() {
        return null;
    }
}
