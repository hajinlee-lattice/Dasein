package com.latticeengines.metadata.service;

import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;

public interface DataFeedService {

    DataFeedExecution startExecution(String customerSpace, String datafeedName);

    DataFeed findDataFeedByName(String customerSpace, String datafeedName);

    void updateDataFeed(String customerSpace, DataFeed datafeed);

    DataFeedExecution finishExecution(String customerSpace, String datafeedName);

    DataFeed createDataFeed(String customerSpace, DataFeed datafeed);

}
