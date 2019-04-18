package com.latticeengines.camille.exposed.config.bootstrap;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.config.bootstrap.BootstrapUtil.InstallerAdaptor;
import com.latticeengines.camille.exposed.config.bootstrap.BootstrapUtil.ServiceInstallerAdaptor;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.bootstrap.ServiceInstaller;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceProperties;
import com.latticeengines.domain.exposed.camille.scopes.ServiceScope;

public class ServiceBootstrapManager {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private static Map<String, Bootstrapper> bootstrappers = new ConcurrentHashMap<String, Bootstrapper>();

    public static void register(String serviceName, ServiceProperties properties, ServiceInstaller installer) {
        Bootstrapper bootstrapper = bootstrappers.get(serviceName);
        if (bootstrapper == null) {
            bootstrapper = new Bootstrapper(serviceName, properties, installer);
            bootstrappers.put(serviceName, bootstrapper);
        } else {
            bootstrapper.set(installer, properties);
        }
    }

    public static void bootstrap(ServiceScope scope) throws Exception {
        Bootstrapper bootstrapper = bootstrappers.get(scope.getServiceName());
        if (bootstrapper == null) {
            throw new IllegalArgumentException("Must register installer for service " + scope.getServiceName());
        }
        bootstrapper.bootstrap(scope.getProperties());
    }

    public static void resetAll() {
        bootstrappers.clear();
    }

    public static void reset(String serviceName) {
        bootstrappers.remove(serviceName);
    }

    public static BootstrapState getBootstrapState(String serviceName) throws Exception {
        Path serviceDirectoryPath = PathBuilder.buildServicePath(CamilleEnvironment.getPodId(), serviceName);
        try {
            return BootstrapStateUtil.getState(serviceDirectoryPath);
        } catch (Exception e) {
            throw new Exception(String.format("Error encountered retrieving bootstrap state for service %s",
                    serviceName), e);
        }
    }

    public static Set<String> getRegisteredBootstrappers() {
        return bootstrappers.keySet();
    }

    public static List<AbstractMap.SimpleEntry<String, BootstrapState>> getBootstrapStates() throws Exception {
        Path servicesDirectoryPath = PathBuilder.buildServicesPath(CamilleEnvironment.getPodId());
        Camille camille = CamilleEnvironment.getCamille();

        List<AbstractMap.SimpleEntry<String, BootstrapState>> toReturn = new ArrayList<AbstractMap.SimpleEntry<String, BootstrapState>>();
        try {
            List<AbstractMap.SimpleEntry<Document, Path>> children = camille.getChildren(servicesDirectoryPath);
            for (AbstractMap.SimpleEntry<Document, Path> child : children) {
                String serviceName = child.getValue().getSuffix();
                BootstrapState state = getBootstrapState(serviceName);
                toReturn.add(new AbstractMap.SimpleEntry<String, BootstrapState>(serviceName, state));
            }
        } catch (Exception e) {
            throw new Exception(String.format("Error encountered retrieving bootstrap states for all services"), e);
        }

        return toReturn;
    }

    public static class Bootstrapper {
        private boolean bootstrapped;
        private ServiceProperties properties;
        private ServiceInstaller installer;
        private final String logPrefix;
        private final String serviceName;

        public Bootstrapper(String serviceName, ServiceProperties properties, ServiceInstaller installer) {
            this.properties = properties;
            this.installer = installer;
            this.logPrefix = String.format("[Service=%s] ", serviceName);
            this.serviceName = serviceName;
        }

        public void set(ServiceInstaller installer, ServiceProperties properties) {
            this.installer = installer;
            this.properties = properties;
        }

        public void bootstrap(Map<String, String> bootstrapProperties) throws Exception {
            if (!bootstrapped) {
                synchronized (this) {
                    if (!bootstrapped) {
                        log.info("{}Running bootstrap", logPrefix);
                        install(this.properties.dataVersion, bootstrapProperties);
                        bootstrapped = true;
                    }
                }
            }
        }

        private void install(int executableVersion, Map<String, String> bootstrapProperties) throws Exception {
            Path serviceDirectoryPath = PathBuilder.buildServicePath(CamilleEnvironment.getPodId(), this.serviceName);
            InstallerAdaptor adaptor = new ServiceInstallerAdaptor(installer, serviceName, bootstrapProperties);
            BootstrapUtil.install(adaptor, executableVersion, serviceDirectoryPath, logPrefix);
        }
    }
}
