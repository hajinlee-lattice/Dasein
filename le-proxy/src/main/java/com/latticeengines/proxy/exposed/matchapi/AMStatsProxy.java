package com.latticeengines.proxy.exposed.matchapi;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributeTree;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("amStatsProxy")
public class AMStatsProxy extends BaseRestApiProxy {

    public AMStatsProxy() {
        super(PropertyUtils.getProperty("common.matchapi.url"), "/match/amstats");
    }

    public TopNAttributeTree getTopAttrTree() {
        String url = constructUrl("/topattrs");
        return get("top_attr_tree", url, TopNAttributeTree.class);
    }

    public AccountMasterCube getCube(String query) {
        String url = constructUrl("/cubes");
        return get("am_cube", url, AccountMasterCube.class);
    }

}
