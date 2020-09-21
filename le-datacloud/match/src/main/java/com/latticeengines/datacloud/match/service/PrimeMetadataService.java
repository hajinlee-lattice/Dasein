package com.latticeengines.datacloud.match.service;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.latticeengines.domain.exposed.datacloud.manage.DataBlock;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockMetadataContainer;
import com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn;

public interface PrimeMetadataService {

    String DunsNumber = "duns_number";
    String SubjectName = "primaryname";
    String SubjectCity = "primaryaddr_addrlocality_name";
    String SubjectState = "primaryaddr_addrregion_abbreviatedname";
    String SubjectState2 = "primaryaddr_addrregion_name";
    String SubjectCountry = "countryisoalpha2code";

    DataBlockMetadataContainer getDataBlockMetadata();

    List<DataBlock> getDataBlocks();

    Set<String> getBlocksContainingElements(Collection<String> elementIds);

    List<PrimeColumn> getPrimeColumns(Collection<String> elementIds);

}
