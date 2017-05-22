package com.latticeengines.network.exposed.metadata;

import com.latticeengines.domain.exposed.metadata.StatisticsContainer;

public interface StatisticsContainerInterface {
    StatisticsContainer getStatistics(String customerSpace, String statisticsName);

    StatisticsContainer createOrUpdateStatistics(String customerSpace, StatisticsContainer statistics);

    void deleteStatistics(String customerSpace, String name);
}
