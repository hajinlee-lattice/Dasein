package com.latticeengines.playmaker.service.impl;

import java.util.Date;

import org.apache.commons.lang.StringUtils;

import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;

public class LpiPMUtils {

    public static Date dateFromEpochSeconds(long start) {
        return new Date(start * 1000);
    }

    public static String convertToSFDCFieldType(String sourceLogicalDataType) {
        String type = sourceLogicalDataType;

        if (StringUtils.isNotBlank(sourceLogicalDataType)) {
            sourceLogicalDataType = sourceLogicalDataType.toLowerCase();

            if (sourceLogicalDataType.contains(PlaymakerConstants.VarChar)) {
                type = "nvarchar";
            } else if (sourceLogicalDataType.equals("double")) {
                type = "decimal";
            } else if (sourceLogicalDataType.equals("long")) {
                type = "bigint";
            } else if (sourceLogicalDataType.equals("boolean")) {
                type = "bit";
            }
        } else {
            type = "";
        }

        return type.toUpperCase();
    }

    public static Integer findLengthIfStringType(String sourceLogicalDataType) {
        Integer length = null;

        if (StringUtils.isNotBlank(sourceLogicalDataType)) {
            sourceLogicalDataType = sourceLogicalDataType.toLowerCase();

            if (sourceLogicalDataType.contains(PlaymakerConstants.VarChar)) {
                length = 4000;

                if (sourceLogicalDataType.contains("(")) {

                    sourceLogicalDataType = sourceLogicalDataType.substring(sourceLogicalDataType.indexOf("("));

                    if (sourceLogicalDataType.contains(")")) {

                        sourceLogicalDataType = sourceLogicalDataType.substring(0, sourceLogicalDataType.indexOf(")"));

                        if (StringUtils.isNumeric(sourceLogicalDataType)) {
                            length = Integer.parseInt(sourceLogicalDataType);
                        }
                    }
                }
            }
        }

        return length;
    }

}
