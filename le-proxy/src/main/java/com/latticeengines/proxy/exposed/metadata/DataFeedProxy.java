package com.latticeengines.proxy.exposed.metadata;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;
import com.latticeengines.network.exposed.metadata.DataFeedInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("datafeedProxy")
public class DataFeedProxy extends MicroserviceRestApiProxy implements DataFeedInterface {

    public DataFeedProxy() {
        super("datafeed");
    }

    @Override
    public DataFeedExecution startExecution(String customerSpace, String datafeedName) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeeds/{datafeedName}/startexecution",
                customerSpace, datafeedName);
        return post("startExecution", url, null, DataFeedExecution.class);
    }

    @Override
    public DataFeed findDataFeedByName(String customerSpace, String datafeedName) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeeds/{datafeedName}", customerSpace,
                datafeedName);
        return get("findDataFeedByName", url, DataFeed.class);
    }

    @Override
    public DataFeedExecution finishExecution(String customerSpace, String datafeedName) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeeds/{datafeedName}/finishexecution",
                customerSpace, datafeedName);
        return post("finishExecution", url, null, DataFeedExecution.class);
    }

    @Override
    public DataFeed createDataFeed(String customerSpace, DataFeed datafeed) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeeds", customerSpace);
        return post("createDataFeed", url, datafeed, DataFeed.class);
    }

    @Override
    public DataFeedExecution failExecution(String customerSpace, String datafeedName) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeeds/{datafeedName}/failexecution",
                customerSpace, datafeedName);
        return post("failExecution", url, null, DataFeedExecution.class);
    }

}
