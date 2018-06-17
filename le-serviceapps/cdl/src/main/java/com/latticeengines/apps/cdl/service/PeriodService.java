package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.DataCollection;

public interface PeriodService {

    List<String> getPeriodNames();

    List<PeriodStrategy> getPeriodStrategies();

    String getEvaluationDate();

    int getMaxPeriodId(String customerSpace, PeriodStrategy periodStrategy, DataCollection.Version version);

    Map<PeriodStrategy.Template, Integer> getPeriodId(String date, PeriodStrategy periodStrategy);

    PeriodStrategy getApsRollupPeriod(DataCollection.Version version);
}
