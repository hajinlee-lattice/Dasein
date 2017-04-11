package com.latticeengines.common.exposed.util;

import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;


public class ProxyUtils {

    @SuppressWarnings("unchecked")
    public static <T> T getTargetObject(Object proxy) throws Exception {
        if (AopUtils.isJdkDynamicProxy(proxy)) {
            return (T) ((Advised) proxy).getTargetSource().getTarget();
        } else {
            return (T) proxy;
        }
    }

}
