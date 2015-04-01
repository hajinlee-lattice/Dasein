package com.latticeengines.marketoadapter;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.latticeengines.camille.exposed.config.ConfigurationController;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ServiceScope;

@Service
public class RecordDispatcher {
    @PostConstruct
    public void Initialize() {
        destinations = new HashMap<String, RecordDestination>();
        destinations.put("Console", consoleDestination);
        destinations.put("Skald", skaldDestination);
    }

    public String receiveRecord(String key, Map<String, Object> record) {
        // TODO: Add a caching layer for the key definitions file.
        KeyDefinitions definitions;
        try {
            ServiceScope scope = new ServiceScope(MarketoAdapterBootstrapper.SERVICE_NAME,
                    MarketoAdapterBootstrapper.DATA_VERSION);
            ConfigurationController<ServiceScope> controller = ConfigurationController.construct(scope);

            Path path = new Path("/KeyDefinitions.json");
            definitions = JsonUtils.deserialize(controller.get(path).getData(), KeyDefinitions.class);
        } catch (Exception ex) {
            throw new RuntimeException("Unable to retrieve key definitions", ex);
        }

        KeyDefinitions.Value settings = definitions.get(key);
        if (settings == null) {
            throw new RuntimeException("Lattice Key does not match a known customer");
        }

        RecordDestination destination = destinations.get(settings.destination);
        if (destination == null) {
            throw new RuntimeException("Received a request with an unknown destination");
        }

        return destination.receiveRecord(settings.space, record);
    }

    @Autowired
    private ConsoleDestination consoleDestination;
    @Autowired
    private SkaldDestination skaldDestination;

    private Map<String, RecordDestination> destinations;
}
