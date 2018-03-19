package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;

public interface CdlModelMetadataService {

    List<Table> cloneTrainingTargetTable(ModelSummary modelSummary);

    List<Table> getTrainingTargetTableFromModelId(ModelSummary modelSummary);

}
