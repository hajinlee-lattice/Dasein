package com.latticeengines.propdata.core.service.impl;

import org.apache.commons.lang.StringUtils;

import com.latticeengines.camille.exposed.CamilleEnvironment;

public class HdfsPodContext {

    private static ThreadLocal<String> podId = new ThreadLocal<>();

    public static void changeHdfsPodId(String podId) {
        HdfsPodContext.podId.set(podId);
    }

    public static String getHdfsPodId() {
        if (StringUtils.isEmpty(HdfsPodContext.podId.get())) {
            HdfsPodContext.changeHdfsPodId(CamilleEnvironment.getPodId());
        }
        return HdfsPodContext.podId.get();
    }

}
