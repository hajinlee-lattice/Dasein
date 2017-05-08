package com.latticeengines.proxy.exposed.matchapi;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFactQuery;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributeTree;
import com.latticeengines.network.exposed.propdata.AMStatsInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("amStatsProxy")
public class AMStatsProxy extends BaseRestApiProxy implements AMStatsInterface {

    public AMStatsProxy() {
        super(PropertyUtils.getProperty("common.matchapi.url"), "/match/amstats");
    }

    public TopNAttributeTree getTopAttrTree() {
        String url = constructUrl("/topattrs");
        return get("top_attr_tree", url, TopNAttributeTree.class);
    }

    public AccountMasterCube getCube(AccountMasterFactQuery query, boolean considerOnlyEnrichments) {
        String url = constructUrl("/cubes?considerOnlyEnrichments={considerOnlyEnrichments}", considerOnlyEnrichments);
        return post("am_cube", url, query, AccountMasterCube.class);
    }

}
