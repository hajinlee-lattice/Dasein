package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;

public interface PeriodService {

    List<String> getPeriodNames();

    List<PeriodStrategy> getPeriodStrategies();

    String getEvaluationDate();

    int getMaxPeriodId(String customerSpace, PeriodStrategy periodStrategy);

    Map<PeriodStrategy.Template, Integer> getPeriodId(String date, PeriodStrategy periodStrategy);
}
