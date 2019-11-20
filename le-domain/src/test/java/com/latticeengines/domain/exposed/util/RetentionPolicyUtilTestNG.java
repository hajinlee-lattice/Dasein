package com.latticeengines.domain.exposed.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.rention.RetentionPolicy;
import com.latticeengines.domain.exposed.metadata.rention.RetentionPolicyTimeUnit;

public class RetentionPolicyUtilTestNG {

    @Test(groups = "unit")
    public void test() {
        Assert.assertEquals(RetentionPolicyUtil.getExpireTimeByRetentionPolicyStr(RetentionPolicyUtil.NEVER_EXPIRE_POLICY), -1l);
        Assert.assertEquals(RetentionPolicyUtil.getExpireTimeByRetentionPolicyStr(""), -1l);
        Assert.assertEquals(RetentionPolicyUtil.getExpireTimeByRetentionPolicyStr("KEEP_1_DAY"), 86400000l);
        Assert.assertEquals(RetentionPolicyUtil.getExpireTimeByRetentionPolicyStr("KEEP_2_DAYS"), 2 * 86400000l);
        Assert.assertEquals(RetentionPolicyUtil.getExpireTimeByRetentionPolicyStr("KEEP_1_WEEK"), 7 * 86400000l);
        Assert.assertEquals(RetentionPolicyUtil.getExpireTimeByRetentionPolicyStr("KEEP_2_WEEKS"), 14 * 86400000l);
        Assert.assertEquals(RetentionPolicyUtil.getExpireTimeByRetentionPolicyStr("KEEP_1_MONTH"), 30 * 86400000l);
        Assert.assertEquals(RetentionPolicyUtil.getExpireTimeByRetentionPolicyStr("KEEP_2_MONTHS"), 60 * 86400000l);
        Assert.assertEquals(RetentionPolicyUtil.getExpireTimeByRetentionPolicyStr("KEEP_1_YEAR"), 365 * 86400000l);
        Assert.assertEquals(RetentionPolicyUtil.getExpireTimeByRetentionPolicyStr("KEEP_2_YEARS"), 2 * 365 * 86400000l);

        Assert.assertEquals(RetentionPolicyUtil.getExpireTimeByRetentionPolicy(null), -1l);
        RetentionPolicy retentionPolicy = new RetentionPolicy();
        retentionPolicy.setNoExpire(true);
        Assert.assertEquals(RetentionPolicyUtil.getExpireTimeByRetentionPolicy(retentionPolicy), -1l);

        retentionPolicy.setNoExpire(false);
        Assert.assertEquals(RetentionPolicyUtil.getExpireTimeByRetentionPolicy(retentionPolicy), -1l);

        retentionPolicy.setCount(1);
        Assert.assertEquals(RetentionPolicyUtil.getExpireTimeByRetentionPolicy(retentionPolicy), -1l);

        retentionPolicy.setRetentionPolicyTimeUnit(RetentionPolicyTimeUnit.DAY);
        Assert.assertEquals(RetentionPolicyUtil.getExpireTimeByRetentionPolicy(retentionPolicy), 86400000l);

        retentionPolicy.setCount(2);
        Assert.assertEquals(RetentionPolicyUtil.getExpireTimeByRetentionPolicy(retentionPolicy), 2 * 86400000l);

        setRetentionPolicy(retentionPolicy, 2, RetentionPolicyTimeUnit.WEEK);
        Assert.assertEquals(RetentionPolicyUtil.getExpireTimeByRetentionPolicy(retentionPolicy), 2 * 7 * 86400000l);
        setRetentionPolicy(retentionPolicy, 3, RetentionPolicyTimeUnit.MONTH);
        Assert.assertEquals(RetentionPolicyUtil.getExpireTimeByRetentionPolicy(retentionPolicy), 3 * 30 * 86400000l);
        setRetentionPolicy(retentionPolicy, 2, RetentionPolicyTimeUnit.YEAR);
        Assert.assertEquals(RetentionPolicyUtil.getExpireTimeByRetentionPolicy(retentionPolicy), 2 * 365 * 86400000l);

        retentionPolicy = new RetentionPolicy();
        retentionPolicy.setNoExpire(true);
        Assert.assertEquals(RetentionPolicyUtil.retentionPolicyToStr(retentionPolicy), RetentionPolicyUtil.NEVER_EXPIRE_POLICY);
        retentionPolicy.setNoExpire(false);
        Assert.assertEquals(RetentionPolicyUtil.retentionPolicyToStr(retentionPolicy), RetentionPolicyUtil.NEVER_EXPIRE_POLICY);
        retentionPolicy.setCount(1);
        Assert.assertEquals(RetentionPolicyUtil.retentionPolicyToStr(retentionPolicy), RetentionPolicyUtil.NEVER_EXPIRE_POLICY);
        retentionPolicy.setRetentionPolicyTimeUnit(RetentionPolicyTimeUnit.DAY);
        Assert.assertEquals(RetentionPolicyUtil.retentionPolicyToStr(retentionPolicy), "KEEP_1_DAY");
        setRetentionPolicy(retentionPolicy, 3, RetentionPolicyTimeUnit.DAY);
        Assert.assertEquals(RetentionPolicyUtil.retentionPolicyToStr(retentionPolicy), "KEEP_3_DAYS");
        setRetentionPolicy(retentionPolicy, 1, RetentionPolicyTimeUnit.WEEK);
        Assert.assertEquals(RetentionPolicyUtil.retentionPolicyToStr(retentionPolicy), "KEEP_1_WEEK");
        setRetentionPolicy(retentionPolicy, 3, RetentionPolicyTimeUnit.WEEK);
        Assert.assertEquals(RetentionPolicyUtil.retentionPolicyToStr(retentionPolicy), "KEEP_3_WEEKS");
        setRetentionPolicy(retentionPolicy, 1, RetentionPolicyTimeUnit.MONTH);
        Assert.assertEquals(RetentionPolicyUtil.retentionPolicyToStr(retentionPolicy), "KEEP_1_MONTH");
        setRetentionPolicy(retentionPolicy, 2, RetentionPolicyTimeUnit.MONTH);
        Assert.assertEquals(RetentionPolicyUtil.retentionPolicyToStr(retentionPolicy), "KEEP_2_MONTHS");
        setRetentionPolicy(retentionPolicy, 1, RetentionPolicyTimeUnit.YEAR);
        Assert.assertEquals(RetentionPolicyUtil.retentionPolicyToStr(retentionPolicy), "KEEP_1_YEAR");
        setRetentionPolicy(retentionPolicy, 2, RetentionPolicyTimeUnit.YEAR);
        Assert.assertEquals(RetentionPolicyUtil.retentionPolicyToStr(retentionPolicy), "KEEP_2_YEARS");
    }

    private void setRetentionPolicy(RetentionPolicy retentionPolicy, int count, RetentionPolicyTimeUnit retentionPolicyTimeUnit) {
        retentionPolicy.setCount(count);
        retentionPolicy.setRetentionPolicyTimeUnit(retentionPolicyTimeUnit);
    }
}
