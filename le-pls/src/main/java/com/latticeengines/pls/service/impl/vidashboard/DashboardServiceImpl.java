package com.latticeengines.pls.service.impl.vidashboard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.core.util.UuidUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.admin.LatticeModule;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.dashboard.DashboardResponse;
import com.latticeengines.domain.exposed.cdl.dashboard.TargetAccountList;
import com.latticeengines.domain.exposed.looker.EmbedUrlData;
import com.latticeengines.domain.exposed.looker.EmbedUrlUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.ListSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.datastore.AthenaDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.domain.exposed.metadata.template.CSVAdaptor;
import com.latticeengines.domain.exposed.metadata.template.ImportFieldMapping;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.pls.service.vidashboard.DashboardService;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;

@Component("dashboardService")
public class DashboardServiceImpl implements DashboardService {

    private static final Logger log = LoggerFactory.getLogger(DashboardServiceImpl.class);

    private static final List<String> SSVI_USER_PERMISSIONS = Arrays.asList("see_drill_overlay",
            "see_lookml_dashboards", "access_data");
    private static final List<String> SSVI_DASHBOARDS = Arrays.asList("overview", "accounts_visited", "page_analysis");
    private static final String USER_ATTR_WEB_VISIT_DATA_TABLE = "web_visit_data_table";
    private static final String USER_ATTR_TARGET_ACCOUNT_LIST_TABLE = "target_account_list_table";
    private static final String SSVI_EXTERNAL_SYSTEM_NAME = "SSVI";
    private static final String DEFAULT_TARGET_ACCOUNT_LIST_SEGMENT_NAME = "ssvi_default_target_account_list";

    @Value("${pls.looker.host}")
    private String lookerHost;

    @Value("${pls.looker.secret.encrypted}")
    private String lookerEmbedSecret;

    @Value("${pls.looker.session.seconds:2400}")
    private Long lookerSessionLengthInSeconds;

    @Value("${pls.looker.ssvi.model}")
    private String ssviLookerModelName;

    @Value("${pls.looker.ssvi.usergroup.id}")
    private Integer ssviUserGroupId;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Inject
    private BatonService batonService;

    @Override
    public DashboardResponse getDashboardList(String customerSpace) {
        DashboardResponse res = new DashboardResponse();
        res.setDashboardUrls(getDashboardMap(customerSpace, getWebVisitTableName(customerSpace)));
        return res;
    }

    @Override
    public MetadataSegment createTargetAccountList(@NotNull String customerSpace, String listName) {
        listName = getOrUseDefaultListName(listName);
        return segmentProxy.createOrUpdateListSegment(customerSpace, generateMetadataSegment(customerSpace, listName));
    }

    @Override
    public ListSegment updateTargetAccountListMapping(@NotNull String customerSpace, String listName,
            @NotNull CSVAdaptor csvAdaptor) {
        listName = getOrUseDefaultListName(listName);
        MetadataSegment segment = segmentProxy.getListSegmentByExternalInfo(customerSpace, SSVI_EXTERNAL_SYSTEM_NAME,
                listName);
        if (segment == null) {
            // TODO throw proper UI exception
            log.error("cannot find target account list {}.", listName);
            throw new IllegalArgumentException(String.format("cannot find target account list %s.", listName));
        }
        ListSegment listSegment = segment.getListSegment();
        listSegment.setCsvAdaptor(csvAdaptor);
        return segmentProxy.updateListSegment(customerSpace, listSegment);
    }

    @Override
    public TargetAccountList getTargetAccountList(String customerSpace, String listName) {
        listName = getOrUseDefaultListName(listName);
        MetadataSegment segment = segmentProxy.getListSegmentByExternalInfo(customerSpace, SSVI_EXTERNAL_SYSTEM_NAME,
                listName);
        if (segment == null) {
            // TODO throw proper UI exception
            log.error("cannot find target account list {}.", listName);
            throw new IllegalArgumentException(String.format("cannot find target account list %s.", listName));
        }
        return toTargetAccountList(customerSpace, segment);
    }

    @Override
    public void deleteTargetAccountList(String customerSpace, String listName) {
        TargetAccountList list = getTargetAccountList(customerSpace, listName);
        if (list == null) {
            // TODO throw proper UI exception
            log.error("cannot find target account list {}.", listName);
            throw new IllegalArgumentException(String.format("cannot find target account list %s.", listName));
        }
        segmentProxy.deleteSegmentByName(customerSpace, listName, false);
    }

    private TargetAccountList toTargetAccountList(@NotNull String customerSpace,
            @NotNull MetadataSegment segment) {
        ListSegment listSegment = segment.getListSegment();
        TargetAccountList targetAccountList = new TargetAccountList();
        targetAccountList.setSegmentName(segment.getName());
        targetAccountList.setCsvAdaptor(listSegment.getCsvAdaptor());
        targetAccountList.setS3UploadDropFolder(listSegment.getS3DropFolder());
        Map<String, String> dataTemplates = listSegment.getDataTemplates();
        if (MapUtils.isEmpty(dataTemplates) || !dataTemplates.containsKey(BusinessEntity.Account.name())) {
            log.warn("No account data template in target account list {}", segment.getName());
            return targetAccountList;
        }
        String dataTemplateId = dataTemplates.get(BusinessEntity.Account.name());
        DataUnit dataUnit = dataUnitProxy.getByDataTemplateIdAndRole(customerSpace, dataTemplateId,
                DataUnit.Role.Master);
        if (dataUnit == null) {
            log.warn("No data unit found for target account list {} and data template {}", segment.getName(),
                    dataTemplateId);
            return targetAccountList;
        }

        AthenaDataUnit athenaDataUnit = (AthenaDataUnit) dataUnitProxy.getByNameAndType(customerSpace,
                dataUnit.getName(), DataUnit.StorageType.Athena);
        S3DataUnit dataUnit1 = (S3DataUnit) dataUnit;
        targetAccountList.setTableName(dataUnit1.getName());
        targetAccountList.setS3Path(dataUnit1.getLinkedDir());
        targetAccountList.setHdfsPath(dataUnit1.getLinkedHdfsPath());
        targetAccountList.setAthenaTableName(athenaDataUnit == null ? null : athenaDataUnit.getAthenaTable());
        return targetAccountList;
    }

    private MetadataSegment generateMetadataSegment(String customerSpace, String listName) {
        MetadataSegment metadataSegment = new MetadataSegment();
        metadataSegment.setName(listName);
        metadataSegment.setDisplayName(listName);
        ListSegment listSegment = new ListSegment();
        listSegment.setExternalSystem(SSVI_EXTERNAL_SYSTEM_NAME);
        listSegment.setExternalSegmentId(listName);
        listSegment.setCsvAdaptor(defaultMappings(customerSpace));
        metadataSegment.setListSegment(listSegment);
        return metadataSegment;
    }

    private Map<String, String> getDashboardMap(@NotNull String customerSpace, String webVisitTableName) {
        String tenant = CustomerSpace.shortenCustomerSpace(customerSpace);
        // FIXME add back checks to make sure web visit table exist
        // if (StringUtils.isEmpty(webVisitTableName)) {
        // return null;
        // }
        return SSVI_DASHBOARDS.stream().map(dashboard -> {
            EmbedUrlData data = new EmbedUrlData();
            data.setHost(lookerHost);
            data.setSecret(lookerEmbedSecret);
            data.setExternalUserId(tenant);
            data.setFirstName("SSVI");
            data.setLastName("User");
            data.setGroupIds(Collections.singletonList(ssviUserGroupId));
            data.setPermissions(SSVI_USER_PERMISSIONS);
            data.setModels(Collections.singletonList(ssviLookerModelName));
            data.setSessionLength(lookerSessionLengthInSeconds);
            data.setEmbedUrl(EmbedUrlUtils.embedUrl(ssviLookerModelName, dashboard));
            data.setForceLogoutLogin(true);
            data.setUserAttributes(getUserAttributes(customerSpace, webVisitTableName));
            return Pair.of(dashboard, EmbedUrlUtils.signEmbedDashboardUrl(data));
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    private String getWebVisitTableName(String customerSpace) {
        String tableName = dataCollectionProxy.getTableName(customerSpace, TableRoleInCollection.ConsolidatedWebVisit);
        if (StringUtils.isBlank(tableName)) {
            log.warn("No web visit table found for tenant {}", customerSpace);
            return null;
        }

        AthenaDataUnit unit = (AthenaDataUnit) dataUnitProxy.getByNameAndType(customerSpace, tableName,
                DataUnit.StorageType.Athena);
        if (unit == null) {
            log.warn("No athena data unit found for web visit table {} and tenant {}", tableName, customerSpace);
            return null;
        }

        log.info("Web visit athena table name = {} for tenant {}", unit.getAthenaTable(), customerSpace);
        return unit.getAthenaTable();
    }

    private Map<String, Object> getUserAttributes(String customerSpace, String webVisitTableName) {
        Map<String, Object> userAttrs = new HashMap<>();
        if (StringUtils.isBlank(webVisitTableName)) {
            // FIXME remove mock data
            userAttrs.put(USER_ATTR_WEB_VISIT_DATA_TABLE, "atlas_qa_performance_b3_ssvi_data_v2");
            userAttrs.put(USER_ATTR_TARGET_ACCOUNT_LIST_TABLE, "atlas_qa_performance_b3_account_list_data_v2");
        } else {
            userAttrs.put(USER_ATTR_WEB_VISIT_DATA_TABLE, webVisitTableName);
            String accountTableName = getTargetAccountTableName(customerSpace);
            if (StringUtils.isNotBlank(accountTableName)) {
                userAttrs.put(USER_ATTR_TARGET_ACCOUNT_LIST_TABLE, accountTableName);
            }
        }
        return userAttrs;
    }

    private CSVAdaptor defaultMappings(String tenant) {
        CustomerSpace customerSpace = CustomerSpace.parse(tenant);
        CSVAdaptor adaptor = new CSVAdaptor();
        List<ImportFieldMapping> mappings = new ArrayList<>(Arrays.asList( //
                stringFieldMapping(InterfaceName.DUNS.name(), InterfaceName.DUNS.name()), //
                stringFieldMapping(InterfaceName.Website.name(), InterfaceName.Website.name()), //
                stringFieldMapping(InterfaceName.CompanyName.name(), InterfaceName.CompanyName.name()), //
                stringFieldMapping(InterfaceName.Country.name(), InterfaceName.Country.name()), //
                stringFieldMapping(InterfaceName.State.name(), InterfaceName.State.name()), //
                stringFieldMapping(InterfaceName.City.name(), InterfaceName.City.name()), //
                stringFieldMapping(InterfaceName.Address_Street_1.name(), InterfaceName.Address_Street_1.name()), //
                stringFieldMapping(InterfaceName.PostalCode.name(), InterfaceName.PostalCode.name()) //
        ));
        // TODO change default mapping after matching is added to list segment upload
        if (batonService.hasModule(customerSpace, LatticeModule.CDL)) {
            mappings.add(stringFieldMapping(InterfaceName.AccountId.name(), "account_id"));
        } else {
            mappings.add(stringFieldMapping(InterfaceName.DUNS.name(), "account_id"));
        }
        adaptor.setImportFieldMappings(mappings);
        return adaptor;
    }

    private ImportFieldMapping stringFieldMapping(String userFieldName, String internalFieldName) {
        ImportFieldMapping mapping = new ImportFieldMapping();
        mapping.setFieldName(internalFieldName);
        mapping.setUserFieldName(userFieldName);
        mapping.setFieldType(UserDefinedType.TEXT);
        return mapping;
    }

    private String getOrUseDefaultListName(String listName) {
        return StringUtils.defaultIfBlank(listName, DEFAULT_TARGET_ACCOUNT_LIST_SEGMENT_NAME);
    }

    private String getTargetAccountTableName(String customerSpace) {
        TargetAccountList defaultTargetAccountList = getTargetAccountList(customerSpace, null);
        return defaultTargetAccountList == null ? null : defaultTargetAccountList.getAthenaTableName();
    }
}
