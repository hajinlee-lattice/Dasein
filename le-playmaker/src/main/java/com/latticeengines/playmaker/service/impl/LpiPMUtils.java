package com.latticeengines.playmaker.service.impl;

import java.util.Date;

public class LpiPMUtils {

    public static Date dateFromEpochSeconds(long start) {
        return new Date(start * 1000);
    }

}
