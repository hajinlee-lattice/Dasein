package com.latticeengines.marketoadapter;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.HttpWithRetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

@Service
public class SkaldDestination implements RecordDestination {
    @Override
    public Map<String, Object> receiveRecord(CustomerSpace customerSpace, Map<String, Object> record) {
        String target = "http://" + properties.getSkaldAddress() + "/ScoreRecord";
        Map<String, Object> data = new HashMap<String, Object>();

        if (!record.containsKey(combinationKeyField)) {
            log.error(combinationKeyField + " field was not present in the request for customer : "
                    + customerSpace.toString());
            throw new RuntimeException(combinationKeyField + " field was not present in the request");
        }
        String combinationName = (String) record.get(combinationKeyField);
        record.remove(combinationKeyField);

        data.put("record", record);
        data.put("space", customerSpace);
        data.put("combination", combinationName);

        try {
            String response = HttpWithRetryUtils.executePostRequest(target, data, null);
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> result = mapper.readValue(response, new TypeReference<Map<String, Object>>() {
            });

            return result;
        } catch (Exception ex) {
            throw new RuntimeException("Error connecting to Skald", ex);
        }
    }

    @Autowired
    private MarketoAdapterProperties properties;
    private final String combinationKeyField = "LatticeCombination";
    private static final Log log = LogFactory.getLog(SkaldDestination.class);
}
