package com.latticeengines.apps.cdl.service.impl;


import java.util.Collections;
import java.util.List;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.service.PeriodService;
import com.latticeengines.domain.exposed.query.TimeFilter;

@Service("periodService")
public class PeriodServiceImpl implements PeriodService {

    @Override
    public List<String> getPeriodNames() {
        return Collections.singletonList(TimeFilter.Period.Month.name());
    }

}
