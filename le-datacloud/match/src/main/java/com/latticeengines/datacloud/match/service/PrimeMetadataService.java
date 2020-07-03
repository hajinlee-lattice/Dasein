package com.latticeengines.datacloud.match.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.DataBlock;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockMetadataContainer;

public interface PrimeMetadataService {

    DataBlockMetadataContainer getDataBlockMetadata();

    List<DataBlock> getDataBlocks();

    DataBlockEntitlementContainer getBaseEntitlement();

}
