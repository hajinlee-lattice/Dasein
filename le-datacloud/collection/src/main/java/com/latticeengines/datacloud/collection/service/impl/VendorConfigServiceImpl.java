package com.latticeengines.datacloud.collection.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.VendorConfigMgr;
import com.latticeengines.datacloud.collection.service.VendorConfigService;
import com.latticeengines.ldc_collectiondb.entity.VendorConfig;

@Component
public class VendorConfigServiceImpl implements VendorConfigService {
    private static final String VENDOR_ALEXA = "ALEXA";
    private static final String VENDOR_BUILTWITH = "BUILTWITH";
    private static final String VENDOR_COMPETE = "COMPETE";
    private static final String VENDOR_FEATURE = "FEATURE";
    private static final String VENDOR_HPA_NEW = "HPA_NEW";
    private static final String VENDOR_ORBI_V2 = "ORBINTELLIGENCEV2";
    private static final String VENDOR_SEMRUSH = "SEMRUSH";

    private static final int DEF_COLLECTION_BATCH = 8192;
    private static final int DEF_MAX_RETRIES = 3;

    @Inject
    private VendorConfigMgr vendorConfigMgr;

    private Map<String, VendorConfig> vendorConfigs;
    private List<String> vendors;

    private boolean ready = false;

    private List<VendorConfig> fetchVendorConfigs() {
        List<VendorConfig> all = vendorConfigMgr.findAll();
        if (all != null && all.size() != 0)
            return all;

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

        return vendorConfigMgr.findAll();
    }

    public void init() {
        if (ready)
            return;

        List<VendorConfig> allConfigs = fetchVendorConfigs();

        Map<String, VendorConfig> tempVendorConfigs = new HashMap<>();
        List<String> tempVendors = new ArrayList<>(allConfigs.size());
        for (int i = 0; i < allConfigs.size(); ++i) {
            VendorConfig vendorConfig = allConfigs.get(i);
            tempVendorConfigs.put(vendorConfig.getVendor(), vendorConfig);
            tempVendors.add(vendorConfig.getVendor());
        }
        vendors = tempVendors;
        vendorConfigs = tempVendorConfigs;
        ready = true;
    }

    @Override
    public List<String> getVendors() {
        init();
        return vendors;
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
        init();
        return vendorConfigs.get(vendor).getDomainField();
    }

    @Override
    public String getDomainCheckField(String vendor) {
        init();
        return vendorConfigs.get(vendor).getDomainCheckField();
    }

    @Override
    public long getCollectingFreq(String vendor) {
        init();
        return vendorConfigs.get(vendor).getCollectingFreq();
    }

    @Override
    public int getMaxActiveTasks(String vendor) {
        init();
        return vendorConfigs.get(vendor).getMaxActiveTasks();
    }
}
