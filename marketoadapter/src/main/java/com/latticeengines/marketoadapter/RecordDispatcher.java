package com.latticeengines.marketoadapter;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class RecordDispatcher {
    @PostConstruct
    public void Initialize() {
        destinations = new HashMap<String, RecordDestination>();
        destinations.put("Console", consoleDestination);
        destinations.put("Skald", skaldDestination);
    }

    public String receiveRecord(String key, Map<String, Object> record) {
        // TODO: Add rate limiting.

        CustomerSettings settings = manager.getCustomerSettingsByKey(key);
        if (settings == null) {
            log.warn("Received a request with unknown key: " + key);
            throw new RuntimeException("Lattice Key does not match a known customer");
        }

        RecordDestination destination = destinations.get(settings.destination);
        if (destination == null) {
            log.warn("Received a request with unknown destination: " + settings.destination);
            throw new RuntimeException("Encountered an internal configuration error");
        }

        return destination.receiveRecord(settings.customerSpace, record);
    }

    @Autowired
    private SettingsManager manager;
    @Autowired
    private ConsoleDestination consoleDestination;
    @Autowired
    private SkaldDestination skaldDestination;

    private Map<String, RecordDestination> destinations;

    private static final Log log = LogFactory.getLog(RecordDispatcher.class);
}
