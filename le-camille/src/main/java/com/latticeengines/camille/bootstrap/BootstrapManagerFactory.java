package com.latticeengines.camille.bootstrap;

import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public class BootstrapManagerFactory {
    public static class NoopBootstrapManager extends BootstrapManager {
        @Override
        public void bootstrap() {
            // TODO Auto-generated method stub

        }
    }

    public static BootstrapManager getBootstrapManager(ConfigurationScope scope) {
        // TODO
        return null;
    }
}
