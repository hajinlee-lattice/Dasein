package com.latticeengines.marketoadapter;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public interface RecordDestination
{
    public String receiveRecord(CustomerSpace customerSpace, Map<String, Object> record);
}
