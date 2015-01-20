package com.latticeengines.marketoadapter;

import java.util.Map;

import org.springframework.stereotype.Service;

@Service
public class ConsoleDestination implements RecordDestination
{
    @Override
    public String receiveRecord(String customerID, Map<String, Object> record) {
        System.out.println("Received record for customer: " + customerID);
        
        for (String key : record.keySet())
        {
            System.out.println(key + ": " + record.get(key));
        }
        
        System.out.println("");
        
        return "Victory!";
    }
}
