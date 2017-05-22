package com.latticeengines.metadata.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.metadata.dao.StatisticsContainerDao;

@Component("statisticsContainerDao")
public class StatisticsContainerDaoImpl extends BaseDaoImpl<StatisticsContainer> implements StatisticsContainerDao {
    @Override
    protected Class<StatisticsContainer> getEntityClass() {
        return StatisticsContainer.class;
    }
}
