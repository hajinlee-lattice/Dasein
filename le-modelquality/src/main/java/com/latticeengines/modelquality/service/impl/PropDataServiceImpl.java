package com.latticeengines.modelquality.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.domain.exposed.modelquality.PropDataMatchType;
import com.latticeengines.modelquality.entitymgr.PropDataEntityMgr;
import com.latticeengines.modelquality.service.PropDataService;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

@Component("propDataService")
public class PropDataServiceImpl extends BaseServiceImpl implements PropDataService {

    @Autowired
    private PropDataEntityMgr propDataEntityMgr;

    @Autowired
    @VisibleForTesting
    private ColumnMetadataProxy columnMetadataProxy;

    private static final Logger log = LoggerFactory.getLogger(PropDataServiceImpl.class);

    @Override
    public PropData createLatestProductionPropData() {
        String version = getLedsVersion();
        String propDataName = "PRODUCTION-" + version.replace('/', '_');
        PropData propData = propDataEntityMgr.findByName(propDataName);

        if (propData != null) {
            return propData;
        }
        propData = new PropData();
        propData.setName(propDataName);
        propData.setDataCloudVersion("2.0.3");
        PropData previousLatest = propDataEntityMgr.getLatestProductionVersion();
        int versionNo = 1;
        if (previousLatest != null) {
            versionNo = previousLatest.getVersion() + 1;
        }
        propData.setVersion(versionNo);
        propDataEntityMgr.create(propData);
        return propData;
    }

    // Custom method with custom logic for stuff Kent wants in the UI
    @Override
    public List<PropData> createLatestProductionPropDatasForUI() {
        String dnbVersion = getLatestDNBVersion();
        final String rtsVersion = "1.0.0";
        final String excludeDomains = "ExcludePublicDomains";
        List<PropData> toReturn = new ArrayList<PropData>();
        for (PropDataMatchType propDataType : PropDataMatchType.values()) {
            PropData propData = null;
            switch (propDataType) {
            case NOMATCH:
                propData = propDataEntityMgr.findByName(PropDataMatchType.NOMATCH.toString());
                if (propData == null) {
                    log.info("Did not find propdata with name: " + PropDataMatchType.NOMATCH.toString());
                    propData = new PropData();
                    propData.setName(PropDataMatchType.NOMATCH.toString());
                    propData.setExcludePropDataColumns(true);
                    propData.setDataCloudVersion("");
                    propDataEntityMgr.create(propData);
                }
                toReturn.add(propData);
                break;
            case RTS:
                // Domains not excluded
                String rtsPropdataWithDomain = PropDataMatchType.RTS.toString() + "-" + rtsVersion;
                propData = propDataEntityMgr.findByName(rtsPropdataWithDomain);
                if (propData == null) {
                    log.info("Did not find propdata with name: " + rtsPropdataWithDomain);
                    propData = new PropData();
                    propData.setName(rtsPropdataWithDomain);
                    propData.setDataCloudVersion(rtsVersion);
                    propDataEntityMgr.create(propData);
                }
                toReturn.add(propData);
                // Domains excluded
                String rtsPropdataDomainExcluded = PropDataMatchType.RTS.toString() + "-" + rtsVersion + "-"
                        + excludeDomains;
                propData = propDataEntityMgr.findByName(rtsPropdataDomainExcluded);
                if (propData == null) {
                    log.info("Did not find propdata with name: " + rtsPropdataDomainExcluded);
                    propData = new PropData();
                    propData.setName(rtsPropdataDomainExcluded);
                    propData.setDataCloudVersion(rtsVersion);
                    propData.setExcludePublicDomains(true);
                    propDataEntityMgr.create(propData);
                }
                toReturn.add(propData);
                break;
            case DNB:
                // Domains not excluded
                String dnbPropdataWithDomain = PropDataMatchType.DNB.toString() + "-" + dnbVersion;
                propData = propDataEntityMgr.findByName(dnbPropdataWithDomain);
                if (propData == null) {
                    log.info("Did not find propdata with name: " + dnbPropdataWithDomain);
                    propData = new PropData();
                    propData.setName(dnbPropdataWithDomain);
                    propData.setDataCloudVersion(dnbVersion);
                    propDataEntityMgr.create(propData);
                }
                toReturn.add(propData);
                // Domains excluded
                String dnbPropdataDomainExcluded = PropDataMatchType.DNB.toString() + "-" + dnbVersion + "-"
                        + excludeDomains;
                propData = propDataEntityMgr.findByName(dnbPropdataDomainExcluded);
                if (propData == null) {
                    log.info("Did not find propdata with name: " + dnbPropdataDomainExcluded);
                    propData = new PropData();
                    propData.setName(dnbPropdataDomainExcluded);
                    propData.setDataCloudVersion(dnbVersion);
                    propData.setExcludePublicDomains(true);
                    propDataEntityMgr.create(propData);
                }
                toReturn.add(propData);
                break;
            default:
                log.error("Unknown PropDataMatchType found: " + propDataType);
            }
        }

        return toReturn;
    }

    protected String getLatestDNBVersion() {
        String toReturn = "2.0.1";
        try {
            DataCloudVersion latestDNBVersion = columnMetadataProxy.latestVersion(null);
            if (latestDNBVersion != null && !latestDNBVersion.getVersion().isEmpty()) {
                toReturn = latestDNBVersion.getVersion();
            } else {
                log.error("Retrieved latest data cloud version was null or empty");
            }
        } catch (Exception e) {
            log.error("Failed to retrieve latest data cloud version");
        }

        return toReturn;
    }
}
