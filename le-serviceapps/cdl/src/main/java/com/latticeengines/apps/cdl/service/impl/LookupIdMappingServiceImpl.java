package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.service.CDLExternalSystemService;
import com.latticeengines.apps.cdl.service.LookupIdMappingService;
import com.latticeengines.apps.core.entitymgr.DropBoxEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemMapping;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.DropBox;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.LookupIdMapUtils;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.remote.tray.TraySettings;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.remote.exposed.service.tray.TrayService;

@Component("lookupIdMappingService")
public class LookupIdMappingServiceImpl implements LookupIdMappingService {
    private static final Logger log = LoggerFactory.getLogger(LookupIdMappingServiceImpl.class);

    @Inject
    private CDLExternalSystemService externalSystemService;

    @Inject
    private TrayService trayService;

    @Inject
    private LookupIdMappingEntityMgr lookupIdMappingEntityMgr;

    @Inject
    private DropBoxEntityMgr dropBoxEntityMgr;

    @Value("${aws.customer.s3.bucket}")
    private String s3CustomerBucket;

    @Value("${aws.customer.export.s3.bucket}")
    private String s3CustomerExportBucket;

    private final HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder();

    @Override
    public Map<String, List<LookupIdMap>> getLookupIdsMapping(CDLExternalSystemType externalSystemType, String sortby,
            boolean descending) {
        List<LookupIdMap> configs = lookupIdMappingEntityMgr.getLookupIdMappings(externalSystemType, sortby, descending)
                .stream().map(this::populateExportFolder)
                .filter(c -> externalSystemType == null || c.getExternalSystemType() == externalSystemType)
                .collect(Collectors.toList());

        Map<String, List<LookupIdMap>> toReturn = LookupIdMapUtils.listToMap(configs);

        if ((externalSystemType == null || externalSystemType == CDLExternalSystemType.FILE_SYSTEM)
                && !toReturn.containsKey(CDLExternalSystemType.FILE_SYSTEM.name())) {
            // Every tenant should have an AWS S3 connection, set one up if its
            // missing for this tenant
            log.info("No FileSystem connection found, creating it now");
            LookupIdMap awsS3 = new LookupIdMap();
            awsS3.setDescription("Lattice S3 dropfolder connection");
            awsS3.setExternalSystemType(CDLExternalSystemType.FILE_SYSTEM);
            awsS3.setExternalSystemName(CDLExternalSystemName.AWS_S3);
            awsS3.setOrgId(CDLConstants.LATTICE_S3_ORG_ID);
            awsS3.setOrgName(CDLConstants.LATTICE_S3_ORG_NAME);
            awsS3 = lookupIdMappingEntityMgr.createExternalSystem(awsS3);
            awsS3 = populateExportFolder(awsS3);

            toReturn.put(CDLExternalSystemType.FILE_SYSTEM.name(), Collections.singletonList(awsS3));
        }
        return toReturn;
    }

    @Override
    public LookupIdMap registerExternalSystem(LookupIdMap lookupIdMap) {
        if (StringUtils.isEmpty(lookupIdMap.getEndDestinationId())) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "Cannot register new system without external system id." });
        }

        LookupIdMap existingLookupIdMap = findExistingLookupIdMap(lookupIdMap);
        if (existingLookupIdMap == null) {
            if (lookupIdMap.getExternalSystemName() == null) {
                throw new LedpException(LedpCode.LEDP_32000,
                        new String[] { "Cannot register new system without external system name. Supported systems ("
                                + Arrays.stream(CDLExternalSystemName.values())
                                        .map(CDLExternalSystemName::getDisplayName).collect(Collectors.joining(", "))
                                + ")" });
            }
            existingLookupIdMap = lookupIdMappingEntityMgr.createExternalSystem(lookupIdMap);
        } else {
            if (lookupIdMap.getExternalSystemType() == CDLExternalSystemType.DSP) {
                throw new LedpException(LedpCode.LEDP_40071);
            }
            existingLookupIdMap.setIsRegistered(true);
            existingLookupIdMap.setExternalAuthentication(lookupIdMap.getExternalAuthentication());
            existingLookupIdMap = lookupIdMappingEntityMgr.updateLookupIdMap(existingLookupIdMap);
        }

        return populateExportFolder(existingLookupIdMap);
    }

    @Override
    public void deregisterExternalSystem(LookupIdMap lookupIdsMap) {
        if (lookupIdsMap == null) {
            return;
        }

        LookupIdMap existingLookupIdMap = lookupIdMappingEntityMgr.getLookupIdMap(lookupIdsMap.getOrgId(),
                lookupIdsMap.getExternalSystemType());
        if (existingLookupIdMap != null && existingLookupIdMap.getIsRegistered()) {
            existingLookupIdMap.setIsRegistered(false);
            lookupIdMappingEntityMgr.updateLookupIdMap(existingLookupIdMap);
        }
    }

    @Override
    public LookupIdMap getLookupIdMap(String id) {
        return populateExportFolder(lookupIdMappingEntityMgr.getLookupIdMap(id));
    }

    @Override
    public LookupIdMap updateLookupIdMap(String id, LookupIdMap lookupIdMap) {
        LookupIdMap existingLookupIdMap = lookupIdMappingEntityMgr.getLookupIdMap(id);
        if (existingLookupIdMap != null) {
            if (lookupIdMap != null) {
                existingLookupIdMap.setOrgName(lookupIdMap.getOrgName());
                existingLookupIdMap.setAccountId(lookupIdMap.getAccountId());
                existingLookupIdMap.setContactId(lookupIdMap.getContactId());
                existingLookupIdMap.setProspectOwner(lookupIdMap.getProspectOwner());
                existingLookupIdMap.setDescription(lookupIdMap.getDescription());
                existingLookupIdMap.setExternalAuthentication(lookupIdMap.getExternalAuthentication());
                existingLookupIdMap.setExportFieldMappings(lookupIdMap.getExportFieldMetadataMappings());
                existingLookupIdMap.setIsRegistered(lookupIdMap.getIsRegistered());
                existingLookupIdMap.setOrgId(lookupIdMap.getOrgId());
            } else {
                throw new LedpException(LedpCode.LEDP_40099);
            }
        } else {
            throw new LedpException(LedpCode.LEDP_40098, new String[] { id });
        }

        return lookupIdMappingEntityMgr.updateLookupIdMap(existingLookupIdMap);
    }

    @Override
    public void deleteLookupIdMap(String id) {
        lookupIdMappingEntityMgr.deleteLookupIdMap(id);
    }

    @Override
    public void deleteConnection(String lookupIdMapId, TraySettings traySettings) {
        try{
            LookupIdMap map = getLookupIdMap(lookupIdMapId);
            ExternalSystemAuthentication trayAuth = map.getExternalAuthentication();
            if (trayAuth != null) {
                if (trayAuth.getSolutionInstanceId() != null) {
                    trayService.removeSolutionInstance(traySettings);
                    trayAuth.setSolutionInstanceId(null);
                    map.setExternalAuthentication(trayAuth);
                    map.setIsRegistered(false);
                    updateLookupIdMap(lookupIdMapId, map);
                }
                if (trayAuth.getTrayAuthenticationId() != null) {
                    trayService.removeAuthentication(traySettings);
                    trayAuth.setTrayAuthenticationId(null);
                    map.setExternalAuthentication(trayAuth);
                    map.setIsRegistered(false);
                    updateLookupIdMap(lookupIdMapId, map);
                }
            }
            deleteLookupIdMap(lookupIdMapId);
        } catch (Exception ex) {
            log.error("Errors while deleting connection: ", ex);
            throw new LedpException(LedpCode.LEDP_40096, new String[] { lookupIdMapId, ex.getMessage() });
        }
    }

    @Override
    public Map<String, List<CDLExternalSystemMapping>> getAllLookupIds(CDLExternalSystemType externalSystemType,
            AudienceType audienceType) {
        CustomerSpace space = MultiTenantContext.getCustomerSpace();
        Map<String, List<CDLExternalSystemMapping>> result;
        try {
            if (externalSystemType == null) {
                result = externalSystemService.getExternalSystemMap(space.toString(), audienceType.asBusinessEntity());
            } else {
                result = new HashMap<>();
                result.put(externalSystemType.name(), externalSystemService.getExternalSystemByType( //
                        space.toString(), externalSystemType, audienceType.asBusinessEntity()));
            }
        } catch (Exception ex) {
            result = new HashMap<>();
            if (externalSystemType == null || externalSystemType == CDLExternalSystemType.CRM) {
                result.put(CDLExternalSystemType.CRM.name(), new ArrayList<>());
            }
            if (externalSystemType == null || externalSystemType == CDLExternalSystemType.MAP) {
                result.put(CDLExternalSystemType.MAP.name(), new ArrayList<>());
            }
            if (externalSystemType == null || externalSystemType == CDLExternalSystemType.ADS) {
                result.put(CDLExternalSystemType.ADS.name(), new ArrayList<>());
            }
            if (externalSystemType == null || externalSystemType == CDLExternalSystemType.ERP) {
                result.put(CDLExternalSystemType.ERP.name(), new ArrayList<>());
            }
            if (externalSystemType == null || externalSystemType == CDLExternalSystemType.DSP) {
                result.put(CDLExternalSystemType.DSP.name(), new ArrayList<>());
            }
            if (externalSystemType == null || externalSystemType == CDLExternalSystemType.OTHER) {
                result.put(CDLExternalSystemType.OTHER.name(), new ArrayList<>());
            }
            log.error("Errors while retrieving connections, returning default map of empty lists", ex);
        }

        return result;
    }

    @Override
    public List<CDLExternalSystemType> getAllCDLExternalSystemType() {
        return Arrays.asList(CDLExternalSystemType.values());
    }

    @Override
    public LookupIdMap getLookupIdMapByOrgId(String orgId, CDLExternalSystemType externalSystemType) {
        return populateExportFolder(lookupIdMappingEntityMgr.getLookupIdMap(orgId, externalSystemType));
    }

    private LookupIdMap findExistingLookupIdMap(LookupIdMap lookupIdMap) {
        CDLExternalSystemName externalSystemName = lookupIdMap.getExternalSystemName();
        List<LookupIdMap> foundList = lookupIdMappingEntityMgr.getLookupIdMapsByExtSysName(externalSystemName);

        for (LookupIdMap curLookupIdMap : foundList) {
            if (externalSystemName == CDLExternalSystemName.MediaMath) {
                return curLookupIdMap;
            }
            if (lookupIdMap.getEndDestinationId().equals(curLookupIdMap.getEndDestinationId())) {
                return curLookupIdMap;
            }
        }

        return null;
    }

    private LookupIdMap populateExportFolder(LookupIdMap lookupIdMap) {
        if (lookupIdMap != null) {
            DropBox dropbox = dropBoxEntityMgr.getDropBox();
            if (dropbox != null && StringUtils.isNotBlank(dropbox.getDropBox()))
                switch (lookupIdMap.getExternalSystemType()) {
                case MAP:
                case ADS:
                case DSP:
                    lookupIdMap.setExportFolder(getUIFriendlyExportFolder(
                            pathBuilder.getS3AtlasFileExportsDir(s3CustomerExportBucket, dropbox.getDropBox())));
                    break;
                case FILE_SYSTEM:
                    lookupIdMap.setExportFolder(
                            new HdfsToS3PathBuilder().getS3CampaignExportDir(s3CustomerBucket, dropbox.getDropBox())
                                    .replace(getProtocolPrefix(), ""));
                    break;
                default:
                    break;
                }
        }
        return lookupIdMap;
    }

    private String getUIFriendlyExportFolder(String exportFolderPath) {
        return exportFolderPath.replace(getProtocolPrefix(), "");
    }

    private String getProtocolPrefix() {
        return pathBuilder.getProtocol() + pathBuilder.getProtocolSeparator() + pathBuilder.getPathSeparator();
    }
}
