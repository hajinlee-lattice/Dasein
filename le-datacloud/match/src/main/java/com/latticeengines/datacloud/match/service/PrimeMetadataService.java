package com.latticeengines.datacloud.match.service;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.latticeengines.domain.exposed.datacloud.manage.DataBlock;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockMetadataContainer;
import com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn;

public interface PrimeMetadataService {

    String DunsNumber = "duns_number";

    DataBlockMetadataContainer getDataBlockMetadata();

    List<DataBlock> getDataBlocks();

    Set<String> getBlocksContainingElements(Collection<String> elementIds);

    List<PrimeColumn> getPrimeColumns(Collection<String> elementIds);

}
