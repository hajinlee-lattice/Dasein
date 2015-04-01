package com.latticeengines.marketoadapter;

import java.util.Map;

import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

@Service
public class ConsoleDestination implements RecordDestination {
    @Override
    public String receiveRecord(CustomerSpace customerSpace, Map<String, Object> record) {
        System.out.println("Received record for customer: " + customerSpace.toString());

        for (String key : record.keySet()) {
            System.out.println(key + ": " + record.get(key));
        }

        System.out.println("");

        return "Victory!";
    }
}
