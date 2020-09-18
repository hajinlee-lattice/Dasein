package com.latticeengines.proxy.exposed.matchapi;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.DataBlock;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockMetadataContainer;
import com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn;

public interface PrimeMetadataProxy {

    List<DataBlock> getBlockElements(List<String> blockIds);
    DataBlockMetadataContainer getBlockMetadata();
    List<PrimeColumn> getPrimeColumns(List<String> elementIds);
    List<PrimeColumn> getCandidateColumns();
    DataBlockEntitlementContainer enrichEntitlementContainerWithElements(DataBlockEntitlementContainer container);

}
