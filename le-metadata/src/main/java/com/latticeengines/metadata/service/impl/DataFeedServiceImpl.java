package com.latticeengines.metadata.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.metadata.entitymgr.DataFeedEntityMgr;
import com.latticeengines.metadata.service.DataFeedService;

@Component("datafeedService")
public class DataFeedServiceImpl implements DataFeedService {

    @Autowired
    private DataFeedEntityMgr datafeedEntityMgr;

    @Override
    public boolean startExecution(String customerSpace, String datafeedName) {
        return datafeedEntityMgr.startExecution(datafeedName);
    }
}
