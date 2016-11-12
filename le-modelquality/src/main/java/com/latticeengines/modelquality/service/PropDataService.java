package com.latticeengines.modelquality.service;

import java.util.List;

import com.latticeengines.domain.exposed.modelquality.PropData;

public interface PropDataService {

    PropData createLatestProductionPropData();

    List<PropData> createLatestProductionPropDatasForUI();
}
