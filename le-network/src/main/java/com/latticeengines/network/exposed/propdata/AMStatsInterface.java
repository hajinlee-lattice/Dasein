package com.latticeengines.network.exposed.propdata;

import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFactQuery;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributeTree;

public interface AMStatsInterface {
    TopNAttributeTree getTopAttrTree();

    public AccountMasterCube getCube(AccountMasterFactQuery query, boolean considerOnlyEnrichments);
}
