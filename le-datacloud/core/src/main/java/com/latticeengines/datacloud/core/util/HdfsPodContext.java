package com.latticeengines.datacloud.core.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.CamilleEnvironment;

public class HdfsPodContext {
    private static Logger log = LoggerFactory.getLogger(HdfsPodContext.class);

    private static ThreadLocal<String> podId = new ThreadLocal<>();

    public static void changeHdfsPodId(String podId) {
        HdfsPodContext.podId.set(podId);
        log.info("Switched hdfs pod to " + podId);
    }

    public static String getHdfsPodId() {
        if (StringUtils.isEmpty(HdfsPodContext.podId.get())) {
            HdfsPodContext.changeHdfsPodId(CamilleEnvironment.getPodId());
        }
        return HdfsPodContext.podId.get();
    }

    public static String getDefaultHdfsPodId() {
        return CamilleEnvironment.getPodId();
    }

}
