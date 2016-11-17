package com.latticeengines.network.exposed.modelquality;

import java.util.List;

import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.DataSetTenantType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

public interface ModelQualityDataSetInterface {

    List<DataSet> getDataSets();

    String createDataSet(DataSet dataset);

    DataSet getDataSetByName(String dataSetName);

    String createDataSetFromTenant(String tenantName, DataSetTenantType tenantType, String modelID,
            SchemaInterpretation schemaInterpretation, String playExternalID);
}
