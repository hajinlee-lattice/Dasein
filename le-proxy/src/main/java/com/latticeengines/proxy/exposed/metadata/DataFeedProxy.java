package com.latticeengines.proxy.exposed.metadata;

import org.springframework.stereotype.Component;

import com.latticeengines.network.exposed.metadata.DataFeedInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("datafeedProxy")
public class DataFeedProxy extends MicroserviceRestApiProxy implements DataFeedInterface {

    public DataFeedProxy() {
        super("datafeed");
    }

    @Override
    public Boolean startExecution(String customerSpace, String datafeedName) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeeds/{datafeedName}/startexecution",
                customerSpace, datafeedName);
        return post("createImportTable", url, null, Boolean.class);
    }

}
