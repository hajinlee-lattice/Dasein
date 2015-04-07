package com.latticeengines.marketoadapter;

import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

@Service
public class ConsoleDestination implements RecordDestination {
    @Override
    public Map<String, Object> receiveRecord(CustomerSpace customerSpace, Map<String, Object> record) {
        System.out.println("");
        System.out.println("Received record for customer: " + customerSpace.toString());

        for (String key : record.keySet()) {
            System.out.println(key + ": " + record.get(key));
        }

        System.out.println("");

        Map<String, Object> result = new HashMap<String, Object>();
        result.put("Test", "Victory!");
        return result;
    }
}
