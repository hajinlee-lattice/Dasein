package com.latticeengines.network.exposed.modelquality;

import java.util.List;

import com.latticeengines.domain.exposed.modelquality.DataSet;

public interface ModelQualityDataSetInterface {

    List<DataSet> getDataSets();

    String createDataSet(DataSet dataset);

    DataSet getDataSetByName(String dataSetName);
}
