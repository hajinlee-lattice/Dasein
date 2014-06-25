package com.latticeengines.skald;

import java.util.Map;

public interface RecordDestination
{
    public String receiveRecord(String customerID, Map<String, Object> record);
}
