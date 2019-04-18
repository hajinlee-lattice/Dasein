package com.latticeengines.network.exposed.propdata;

import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFactQuery;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributeTree;

public interface AMStatsInterface {

    TopNAttributeTree getTopAttrTree();

    AccountMasterCube getCube(AccountMasterFactQuery query, boolean considerOnlyEnrichments);

}
