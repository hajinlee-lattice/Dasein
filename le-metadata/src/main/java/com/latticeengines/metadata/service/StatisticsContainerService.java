package com.latticeengines.metadata.service;

import com.latticeengines.domain.exposed.metadata.StatisticsContainer;

public interface StatisticsContainerService {
    StatisticsContainer findByName(String customerSpace, String statisticsName);

    StatisticsContainer createOrUpdate(String customerSpace, StatisticsContainer container);

    void delete(String customerSpace, String name);
}
