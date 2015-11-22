package com.latticeengines.pls.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.TargetMarketStatistics;
import com.latticeengines.pls.dao.TargetMarketStatisticsDao;

@Component("targetMarketStatisticsDao")
public class TargetMarketStatisticsDaoImpl extends BaseDaoImpl<TargetMarketStatistics> implements
    TargetMarketStatisticsDao {

    @Override
    protected Class<TargetMarketStatistics> getEntityClass() {
        return TargetMarketStatistics.class;
    }

}
