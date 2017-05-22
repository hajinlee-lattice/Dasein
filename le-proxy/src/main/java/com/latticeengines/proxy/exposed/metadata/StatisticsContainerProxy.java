package com.latticeengines.proxy.exposed.metadata;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.network.exposed.metadata.StatisticsContainerInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("statisticsContainerProxy")
public class StatisticsContainerProxy extends MicroserviceRestApiProxy implements StatisticsContainerInterface {

    protected StatisticsContainerProxy() {
        super("metadata");
    }

    @Override
    public StatisticsContainer createOrUpdateStatistics(String customerSpace, StatisticsContainer statistics) {
        String url = constructUrl("/customerspaces/{customerSpace}/statistics/", customerSpace);
        return post("createOrUpdateStatistics", url, statistics, StatisticsContainer.class);
    }

    @Override
    public StatisticsContainer getStatistics(String customerSpace, String statisticsName) {
        String url = constructUrl("/customerspaces/{customerSpace}/statistics/{statisticsName}", customerSpace,
                statisticsName);
        return get("getStatistics", url, StatisticsContainer.class);
    }

    @Override
    public void deleteStatistics(String customerSpace, String statisticsName) {
        String url = constructUrl("/customerspaces/{customerSpace}/statistics/{statisticsName}", customerSpace,
                statisticsName);
        delete("deleteStatistics", url);
    }

}
