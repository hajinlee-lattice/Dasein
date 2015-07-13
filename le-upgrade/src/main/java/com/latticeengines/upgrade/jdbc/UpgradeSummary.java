package com.latticeengines.upgrade.jdbc;

import org.joda.time.DateTime;

public class UpgradeSummary {

    public String tenantName;
    public String activeModelGuid;
    public int modelsInHdfs = 0;
    public int modelsummariesInHdfs = 0;
    public int modelsInLp = 0;
    public DateTime upgradedAt = new DateTime();

}
