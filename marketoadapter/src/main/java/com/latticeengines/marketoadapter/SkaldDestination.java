package com.latticeengines.marketoadapter;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.util.HttpWithRetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

@Service
public class SkaldDestination implements RecordDestination {
    @Override
    public String receiveRecord(CustomerSpace customerSpace, Map<String, Object> record) {
        String skaldConnectionString = properties.getSkaldAddress() + "/ScoreRecord";
        Map<String, Object> dataToSend = new HashMap<String, Object>();

        if (!record.containsKey(combinationKeyField)) {
            log.error(combinationKeyField + " field was not present in the request for customer : "
                    + customerSpace.toString());
            throw new RuntimeException(combinationKeyField + " field was not present in the request");
        }
        String combinationName = (String) record.get(combinationKeyField);
        record.remove(combinationKeyField);

        dataToSend.put("record", record);
        dataToSend.put("space", customerSpace);
        dataToSend.put("combination", combinationName);

        try {
            return HttpWithRetryUtils.executePostRequest(skaldConnectionString, dataToSend, null);
        } catch (Exception e) {
            throw new RuntimeException("Error connecting to Skald:" + e.getMessage());
        }
    }

    @Autowired
    private MarketoAdapterProperties properties;
    private final String combinationKeyField = "LatticeCombination";
    private static final Log log = LogFactory.getLog(SkaldDestination.class);
}
