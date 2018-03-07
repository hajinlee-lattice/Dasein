package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.service.PeriodService;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.query.TimeFilter;

@Service("periodService")
public class PeriodServiceImpl implements PeriodService {

    @Override
    public List<String> getPeriodNames() {
        return Arrays.asList( //
                TimeFilter.Period.Week.name(), //
                TimeFilter.Period.Month.name(), //
                TimeFilter.Period.Quarter.name(), //
                TimeFilter.Period.Year.name() //
        );
    }

    @Override
    public List<PeriodStrategy> getPeriodStrategies() {
        return PeriodStrategy.NATURAL_PERIODS;
    }

}
