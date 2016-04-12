package com.latticeengines.scoringapi.transform.impl;

import java.math.BigInteger;
import java.util.Map;

import com.latticeengines.domain.exposed.scoringapi.FieldType;

/* This class re-implements the Python2 function encoder.py which is defined in the following places
 * 1. /le-scoring/src/test/resources/com/latticeengines/scoring/models/supportedFiles/encoder.py
 * 2. /le-dataplatform/src/main/python/pipeline/encoder.py
 */

public class Encoder {

    public static java.lang.Object encode(Map<String, java.lang.Object> arguments, Map<String, java.lang.Object> record, FieldType returnType) {
        String column = (String) arguments.get("column");
        java.lang.Object value = record.get(column);

        String valueAsString;
        if(value != null)
            valueAsString = value.toString();
        else
            valueAsString = "NULL";

        if( valueAsString == null || valueAsString.equals("None") == true || valueAsString.equals("null") || valueAsString.equals("NULL")) {
            valueAsString = "NULL";
            return "1463472903";
        }

        if(value.getClass() == Boolean.class) {
            if(valueAsString.toString().equals("0") == true)
                return "0";
            else if(valueAsString.toString().equals("1") == true)
                return "1";
            else if(valueAsString.toString().toLowerCase().equals("true") == true)
                return "1";
            else if(valueAsString.toString().toLowerCase().equals("false") == true)
                return "0";
        }

        if(value.getClass() == Integer.class || value.getClass() == Long.class || value.getClass() == Float.class ||
                value.getClass() == Double.class) {
            if(valueAsString.matches("^-?\\d+$") == true || valueAsString.matches("^-?\\d+.\\d+$") == true  ||
                    valueAsString.equals("false") == true || valueAsString.equals("true") == true ) {
                try{
                    Double d = Double.parseDouble(valueAsString);
                    if(d.isNaN()) {
                        valueAsString = "NULL";
                    } else {
                        return valueAsString;
                    }
                } catch(Exception e){
                    valueAsString = "NULL";
                }
            }
        }

        BigInteger code = BigInteger.ZERO;
        for(int i=0;i<valueAsString.length();i++) {
            code = BigInteger.valueOf(valueAsString.charAt(i)).add(code.shiftLeft(6)).add(code.shiftLeft(16)).subtract(code);
        }
        code = code.and(BigInteger.valueOf(4294967295l));

        return code;
    }

}
