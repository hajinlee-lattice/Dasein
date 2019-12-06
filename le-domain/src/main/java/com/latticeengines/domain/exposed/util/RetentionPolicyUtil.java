package com.latticeengines.domain.exposed.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicy;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicyTimeUnit;

public class RetentionPolicyUtil {

    public static String NEVER_EXPIRE_POLICY = "KEEP_FOREVER";

    public static RetentionPolicy noExpireRetentionPolicy;

    static {
        noExpireRetentionPolicy = new RetentionPolicy();
        noExpireRetentionPolicy.setNoExpire(true);
    }

    private static final Logger log = LoggerFactory.getLogger(RetentionPolicyUtil.class);

    public static long getExpireTimeByRetentionPolicyStr(String retentionPolicyStr) {
        long expireTime = -1;
        RetentionPolicy retentionPolicy = strToRetentionPolicy(retentionPolicyStr);
        if (retentionPolicy.isNoExpire()) {
            return expireTime;
        }
        expireTime = retentionPolicy.getCount() * retentionPolicy.getRetentionPolicyTimeUnit().getMilliseconds();
        return expireTime;
    }

    public static long getExpireTimeByRetentionPolicy(RetentionPolicy retentionPolicy) {
        long expireTime = -1;
        if (retentionPolicy == null || retentionPolicy.isNoExpire()) {
            return expireTime;
        }
        int count = retentionPolicy.getCount();
        if (count <= 0) {
            return expireTime;
        }
        RetentionPolicyTimeUnit retentionPolicyTimeUnit = retentionPolicy.getRetentionPolicyTimeUnit();
        if (retentionPolicyTimeUnit == null) {
            return expireTime;
        }
        expireTime = count * retentionPolicyTimeUnit.getMilliseconds();
        return expireTime;
    }

    public static RetentionPolicy strToRetentionPolicy(String retentionPolicyStr) {
        RetentionPolicy retentionPolicy = new RetentionPolicy();
        try {
            if (StringUtils.isEmpty(retentionPolicyStr) || NEVER_EXPIRE_POLICY.equals(retentionPolicyStr)) {
                return noExpireRetentionPolicy;
            }
            String[] parts = retentionPolicyStr.split("_");
            if (parts.length != 3) {
                log.info(String.format("Invalid retention policy %s found.", retentionPolicy));
                return noExpireRetentionPolicy;
            }
            int count = Integer.valueOf(parts[1]);
            RetentionPolicyTimeUnit retentionPolicyTimeUnit = RetentionPolicyTimeUnit.fromName(parts[2]);
            return toRetentionPolicy(count, retentionPolicyTimeUnit);
        } catch (Exception e) {
            log.error(String.format("Exception %s found when parse retention policy.", e.getMessage()));
            return noExpireRetentionPolicy;
        }
    }

    public static RetentionPolicy toRetentionPolicy(int count, RetentionPolicyTimeUnit retentionPolicyTimeUnit) {
        RetentionPolicy retentionPolicy = new RetentionPolicy();
        if (count <= 0) {
            return noExpireRetentionPolicy;
        }
        if (retentionPolicyTimeUnit == null) {
            return noExpireRetentionPolicy;
        }
        retentionPolicy.setCount(count);
        retentionPolicy.setRetentionPolicyTimeUnit(retentionPolicyTimeUnit);
        return retentionPolicy;
    }

    public static String toRetentionPolicyStr(int count, RetentionPolicyTimeUnit retentionPolicyTimeUnit) {
        if (count <= 0) {
            return NEVER_EXPIRE_POLICY;
        }
        if (retentionPolicyTimeUnit == null) {
            return NEVER_EXPIRE_POLICY;
        }
        StringBuffer result = new StringBuffer("KEEP_");
        result.append(count);
        result.append("_");
        appendRetentionPolicyTimeUnitName(result, count, retentionPolicyTimeUnit);
        return result.toString();
    }

    private static void appendRetentionPolicyTimeUnitName(StringBuffer result, int count, RetentionPolicyTimeUnit retentionPolicyTimeUnit) {
        result.append(retentionPolicyTimeUnit.name());
        if (count > 1) {
            result.append("S");
        }
    }

    public static String retentionPolicyToStr(RetentionPolicy retentionPolicy) {
        if (retentionPolicy == null || retentionPolicy.isNoExpire()) {
            return NEVER_EXPIRE_POLICY;
        }
        int count = retentionPolicy.getCount();
        RetentionPolicyTimeUnit retentionPolicyTimeUnit = retentionPolicy.getRetentionPolicyTimeUnit();
        return toRetentionPolicyStr(count, retentionPolicyTimeUnit);
    }

}
