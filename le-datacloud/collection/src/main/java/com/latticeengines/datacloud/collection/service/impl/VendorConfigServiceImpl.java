package com.latticeengines.datacloud.collection.service.impl;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.latticeengines.datacloud.collection.service.VendorConfigService;
import com.latticeengines.ldc_collectiondb.entity.VendorConfig;
import com.latticeengines.ldc_collectiondb.entitymgr.VendorConfigMgr;

@Component
public class VendorConfigServiceImpl implements VendorConfigService {

    private static final Logger log = LoggerFactory.getLogger(VendorConfigServiceImpl.class);

    private static final int DEF_COLLECTION_BATCH = 8192;
    private static final int DEF_MAX_RETRIES = 3;

    @Inject
    private VendorConfigMgr vendorConfigMgr;

    private LoadingCache<String, VendorConfig> vendorConfigCache;

    @PostConstruct
    public void postConstruct() {
        vendorConfigCache = Caffeine.newBuilder()
                .maximumSize(20)
                .expireAfterWrite(1, TimeUnit.MINUTES)
                .build(k -> vendorConfigMgr.getVendorConfig(k));
    }

    private List<VendorConfig> fetchVendorConfigs() {

        List<VendorConfig> all = vendorConfigMgr.findAll();
        if (all != null && all.size() != 0) {

            return all;

        }

        log.error("no vendor configurations found");
        return null;
        /*
        long monthInSecs = 30 * 86400;

        VendorConfig builtwithConfig = new VendorConfig();
        builtwithConfig.setVendor(VENDOR_BUILTWITH);
        builtwithConfig.setCollectingFreq(monthInSecs * 6);
        builtwithConfig.setDomainField("Domain");
        builtwithConfig.setDomainCheckField("Technology_Name");
        builtwithConfig.setMaxActiveTasks(1);
        vendorConfigMgr.create(builtwithConfig);

        VendorConfig alexaConfig = new VendorConfig();
        alexaConfig.setVendor(VENDOR_ALEXA);
        alexaConfig.setDomainField("");//fixme: modify when alexa is ready
        alexaConfig.setDomainCheckField("");//fixme: modify when alexa is ready
        alexaConfig.setCollectingFreq(monthInSecs * 3);
        alexaConfig.setMaxActiveTasks(1);
        vendorConfigMgr.create(alexaConfig);

        VendorConfig competeConfig = new VendorConfig();
        competeConfig.setVendor(VENDOR_COMPETE);
        competeConfig.setDomainField("");//fixme: modify when compete is ready
        competeConfig.setDomainCheckField("");//fixme: modify compete alexa is ready
        competeConfig.setCollectingFreq(monthInSecs * 1);
        competeConfig.setMaxActiveTasks(1);
        vendorConfigMgr.create(competeConfig);

        VendorConfig featureConfig = new VendorConfig();
        featureConfig.setVendor(VENDOR_FEATURE);
        featureConfig.setDomainField("");//fixme: modify when feature is ready
        featureConfig.setDomainCheckField("");//fixme: modify when feature is ready
        featureConfig.setCollectingFreq(monthInSecs * 6);
        featureConfig.setMaxActiveTasks(1);
        vendorConfigMgr.create(featureConfig);

        VendorConfig hpaNewConfig = new VendorConfig();
        hpaNewConfig.setVendor(VENDOR_HPA_NEW);
        hpaNewConfig.setDomainField("");//fixme: modify when hpa_new is ready
        hpaNewConfig.setDomainCheckField("");//fixme: modify when hpa_new is ready
        hpaNewConfig.setCollectingFreq(monthInSecs * 6);
        hpaNewConfig.setMaxActiveTasks(1);
        vendorConfigMgr.create(hpaNewConfig);

        VendorConfig orbitV2Config = new VendorConfig();
        orbitV2Config.setVendor(VENDOR_ORBI_V2);
        orbitV2Config.setDomainField("");//fixme: modify when orbit is ready
        orbitV2Config.setDomainCheckField("");//fixme: modify when orbit is ready
        orbitV2Config.setCollectingFreq(monthInSecs * 3);
        orbitV2Config.setMaxActiveTasks(1);
        vendorConfigMgr.create(orbitV2Config);

        VendorConfig semrushConfig = new VendorConfig();
        semrushConfig.setVendor(VENDOR_SEMRUSH);
        semrushConfig.setDomainField("");//fixme: modify when semrush is ready
        semrushConfig.setDomainCheckField("");//fixme: modify when semrush is ready
        semrushConfig.setCollectingFreq(monthInSecs * 1);
        semrushConfig.setMaxActiveTasks(1);
        vendorConfigMgr.create(semrushConfig);

        return vendorConfigMgr.findAll();*/
    }

    @Override
    public List<String> getVendors() {
        List<VendorConfig> configs = vendorConfigMgr.findAll();
        return configs.stream().map(VendorConfig::getVendor).collect(Collectors.toList());
    }

    @Override
    public List<String> getEnabledVendors() {
        List<VendorConfig> configs = vendorConfigMgr.getEnabledVendors();
        return configs.stream().map(VendorConfig::getVendor).collect(Collectors.toList());
    }

    @Override
    public int getDefCollectionBatch() {
        return DEF_COLLECTION_BATCH;
    }

    @Override
    public int getDefMaxRetries() {
        return DEF_MAX_RETRIES;
    }

    @Override
    public String getDomainField(String vendor) {
        VendorConfig config = getVendorConfigFromCache(vendor);
        return config == null ? "" : config.getDomainField();
    }

    @Override
    public String getDomainCheckField(String vendor) {
        VendorConfig config = getVendorConfigFromCache(vendor);
        return config == null ? "" : config.getDomainCheckField();
    }

    @Override
    public long getCollectingFreq(String vendor) {
        VendorConfig config = getVendorConfigFromCache(vendor);
        if (config == null) {
            throw new RuntimeException("Cannot find configuration for vendor " + vendor);
        }
        return config.getCollectingFreq();
    }

    @Override
    public int getMaxActiveTasks(String vendor) {
        VendorConfig config = getVendorConfigFromCache(vendor);
        if (config == null) {
            throw new RuntimeException("Cannot find configuration for vendor " + vendor);
        }
        return config.getMaxActiveTasks();
    }

    private VendorConfig getVendorConfigFromCache(String vendor) {
        return vendorConfigCache.get(vendor);
    }

}
