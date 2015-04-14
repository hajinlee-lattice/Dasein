package com.latticeengines.marketoadapter;

import java.util.Map;

import com.latticeengines.camille.exposed.config.bootstrap.ServiceWarden;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.ServiceInstaller;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceProperties;

public class MarketoAdapterBootstrapper implements ServiceInstaller {
    // TODO Consider the best place for these constants to be declared.
    public static final String SERVICE_NAME = "MarketoAdapter";
    public static final int DATA_VERSION = 1;

    public static void register() {
        MarketoAdapterBootstrapper bootstrapper = new MarketoAdapterBootstrapper();
        ServiceInfo info = new ServiceInfo(new ServiceProperties(DATA_VERSION), null, null, bootstrapper);
        ServiceWarden.registerService(SERVICE_NAME, info);
    }

    @Override
    public DocumentDirectory install(String serviceName, int dataVersion, Map<String, String> properties) {
        Path path = new Path("/KeyDefinitions.json");
        Document definitions = new Document(JsonUtils.serialize(new KeyDefinitions()));

        DocumentDirectory directory = new DocumentDirectory();
        directory.add(path, definitions);

        return directory;
    }
}
