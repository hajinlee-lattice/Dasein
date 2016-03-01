package com.latticeengines.admin.tenant.batonadapter.dante;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component
public class DanteComponent extends LatticeComponent {
    public static final String componentName = "Dante";

    private static final Log LOGGER = LogFactory.getLog(DanteComponent.class);

    @Value("${admin.dante.dryrun}")
    private boolean dryrun;

    @Value("${admin.dante.hosts:}")
    private String danteHosts;

    private LatticeComponentInstaller installer = new DanteInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new DanteUpgrader();

    @Override
    public Set<LatticeProduct> getAssociatedProducts() {
        return new HashSet<>(Arrays.asList(LatticeProduct.LPA, LatticeProduct.LPA3));
    }

    @Override
    public String getName() {
        return componentName;
    }

    @Override
    public void setName(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CustomerSpaceServiceInstaller getInstaller() {
        installer.setDryrun(false);
        return installer;
    }

    @Override
    public CustomerSpaceServiceUpgrader getUpgrader() {
        return upgrader;
    }

    @Override
    public String getVersionString() {
        return null;
    }

    @Override
    public boolean doRegistration() {
        String defaultJson = "dante_default.json";
        String metadataJson = "dante_metadata.json";
        uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
        // register dummy installer if is in dryrun mode
        return dryrun;
    }

    public void wakeUpAppPool() {
        String[] hosts = danteHosts.split(",");
        for (String host : hosts) {
            if (StringUtils.isNotEmpty(host) && host.contains(":\\\\")) {
                try {
                    HttpClientWithOptionalRetryUtils.sendGetRequest(host, false,
                            Collections.<BasicNameValuePair> emptyList());
                    LOGGER.info("Wake up BIS server by sending a GET to " + host);
                } catch (IOException e) {
                    LOGGER.error("Waking up BIS server at " + host + " failed", e);
                }
            }
        }
    }

}
