package com.latticeengines.testframework.exposed.proxy.pls;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlock;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockMetadataContainer;

@Component("testDataBlockProxy")
public class TestDataBlockProxy extends PlsRestApiProxyBase {

    public TestDataBlockProxy() {
        super("pls/data-blocks");
    }

    public DataBlockMetadataContainer getBlockMetadata() {
        String url = constructUrl("/metadata");
        return get("Get block metadata", url, DataBlockMetadataContainer.class);
    }

    public List<DataBlock> getBlocks() {
        String url = constructUrl("/elements");
        List<?> raw = get("Get block-level-element tree", url, List.class);
        return JsonUtils.convertList(raw, DataBlock.class);
    }

    public DataBlockEntitlementContainer getEntitlement() {
        String url = constructUrl("/entitlement");
        return get("Get block drt entitlement", url, DataBlockEntitlementContainer.class);
    }
}
