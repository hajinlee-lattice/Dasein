package com.latticeengines.marketoadapter;

import com.latticeengines.camille.exposed.config.bootstrap.ServiceBootstrapManager;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.ServiceInstaller;

public class MarketoAdapterBootstrapper implements ServiceInstaller {
    // TODO Consider the best place for these constants to be declared.
    public static final String SERVICE_NAME = "MarketoAdapter";
    public static final int DATA_VERSION = 1;

    public static void register() {
        MarketoAdapterBootstrapper bootstrapper = new MarketoAdapterBootstrapper();
        ServiceBootstrapManager.register(SERVICE_NAME, bootstrapper);
    }

    @Override
    public DocumentDirectory install(String service, int dataVersion) {
        Path path = new Path("/KeyDefinitions.json");
        Document definitions = new Document(JsonUtils.serialize(new KeyDefinitions()));

        DocumentDirectory directory = new DocumentDirectory();
        directory.add(path, definitions);

        return directory;
    }
}
