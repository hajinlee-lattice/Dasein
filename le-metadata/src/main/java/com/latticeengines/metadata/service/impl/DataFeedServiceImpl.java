package com.latticeengines.metadata.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;
import com.latticeengines.metadata.entitymgr.DataFeedEntityMgr;
import com.latticeengines.metadata.service.DataFeedService;

@Component("datafeedService")
public class DataFeedServiceImpl implements DataFeedService {

    @Autowired
    private DataFeedEntityMgr datafeedEntityMgr;

    @Override
    public DataFeedExecution startExecution(String customerSpace, String datafeedName) {
        return datafeedEntityMgr.startExecution(datafeedName);
    }

    @Override
    public DataFeed findDataFeedByName(String customerSpace, String datafeedName) {
        return datafeedEntityMgr.findByName(datafeedName);
    }

    @Override
    public void updateDataFeed(String customerSpace, DataFeed datafeed) {
        datafeedEntityMgr.update(datafeed);
    }

    @Override
    public DataFeedExecution finishExecution(String customerSpace, String datafeedName) {
        return datafeedEntityMgr.finishExecution(datafeedName);
    }
}
