package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.DataCollection;

public interface PeriodService {

    List<String> getPeriodNames();

    List<PeriodStrategy> getPeriodStrategies();

    String getEvaluationDate();

    int getEvaluationPeriod(String customerSpace, DataCollection.Version version, PeriodStrategy periodStrategy);

    int getMaxPeriod(String customerSpace, DataCollection.Version version, PeriodStrategy periodStrategy);
}
