package com.latticeengines.proxy.exposed.lp;

import java.util.List;
import java.util.Set;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;

public interface ModelMetadataProxy {

    List<VdbMetadataField> getMetadata(String customerSpace, String modelGuid);

    Table getTrainingTableFromModelId(String customerSpace, String modelGuid);

    List<Attribute> getAttributesFromFields(String customerSpace, List<Attribute> attributes, List<VdbMetadataField> fields);

    Table getEventTableFromModelId(String customerSpace, String modelGuid);

    List<String> getRequiredColumnDisplayNames(String customerSpace, String modelGuid);

    List<Attribute> getRequiredColumns(String customerSpace, String modelGuid);

    Set<String> getLatticeAttributeNames(String customerSpace, String modelGuid);

}
