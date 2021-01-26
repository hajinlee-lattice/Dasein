package com.latticeengines.pls.service.impl.vidashboard;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.core.util.UuidUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.dashboard.DashboardResponse;
import com.latticeengines.domain.exposed.looker.EmbedUrlData;
import com.latticeengines.domain.exposed.looker.EmbedUrlUtils;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.ListSegment;
import com.latticeengines.domain.exposed.metadata.ListSegmentSummary;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.domain.exposed.metadata.template.CSVAdaptor;
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

    @Override
    public DashboardResponse getDashboardList(String customerSpace) {
        DashboardResponse res = new DashboardResponse();
        res.setDashboardUrls(getDashboardMap(customerSpace));
        return res;
    }

    @Override
    public MetadataSegment createListSegment(String customerSpace, String segmentName) {
        MetadataSegment segment = segmentProxy.createOrUpdateListSegment(customerSpace,
                generateMetadataSegment(segmentName));
        DataCollectionStatus status = dataCollectionProxy.getOrCreateDataCollectionStatus(customerSpace, null);
        List<String> listSegmentNames = status.getListSegmentNames();
        listSegmentNames.add(segmentName);
        status.setListSegmentNames(listSegmentNames);
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(customerSpace, status, null);
        return segment;
    }

    @Override
    public ListSegment updateSegmentFieldMapping(String customerSpace, String segmentName, CSVAdaptor csvAdaptor) {
        DataCollectionStatus status = dataCollectionProxy.getOrCreateDataCollectionStatus(customerSpace, null);
        List<String> listSegmentNames = status.getListSegmentNames();
        if (!listSegmentNames.contains(segmentName)) {
            log.error("Can't find list Segment, segmentName is {}.", segmentName);
            throw new IllegalArgumentException(String.format("Can't find list Segment, segmentName is %s.",
                    segmentName));
        }
        MetadataSegment segment = segmentProxy.getListSegmentByName(customerSpace, segmentName);
        if (segment == null) {
            log.error("Can't find list Segment, segmentName is {}.", segmentName);
            throw new IllegalArgumentException(String.format("Can't find list Segment, segmentName is %s.",
                    segmentName));
        }
        ListSegment listSegment = segment.getListSegment();
        listSegment.setCsvAdaptor(csvAdaptor);
        return segmentProxy.updateListSegment(customerSpace, listSegment);
    }

    @Override
    public ListSegmentSummary getListSegmentMappings(String customerSpace, String segmentName) {
        DataCollectionStatus status = dataCollectionProxy.getOrCreateDataCollectionStatus(customerSpace, null);
        List<String> listSegmentNames = status.getListSegmentNames();
        if (!listSegmentNames.contains(segmentName)) {
            log.error("Can't find list Segment, segmentName is {}.", segmentName);
            throw new IllegalArgumentException(String.format("Can't find list Segment, segmentName is %s.",
                    segmentName));
        }
        MetadataSegment segment = segmentProxy.getListSegmentByName(customerSpace, segmentName);
        return generateListSegmentSummary(customerSpace, segment, false);
    }

    @Override
    public ListSegmentSummary getListSegmentSummary(String customerSpace, String segmentName) {
        DataCollectionStatus status = dataCollectionProxy.getOrCreateDataCollectionStatus(customerSpace, null);
        List<String> listSegmentNames = status.getListSegmentNames();
        if (!listSegmentNames.contains(segmentName)) {
            log.error("Can't find list Segment, segmentName is {}.", segmentName);
            throw new IllegalArgumentException(String.format("Can't find list Segment, segmentName is %s.",
                    segmentName));
        }
        MetadataSegment segment = segmentProxy.getListSegmentByName(customerSpace, segmentName);
        return generateListSegmentSummary(customerSpace, segment, true);
    }

    @Override
    public void deleteListSegment(String customerSpace, String segmentName) {
        DataCollectionStatus status = dataCollectionProxy.getOrCreateDataCollectionStatus(customerSpace, null);
        List<String> listSegmentNames = status.getListSegmentNames();
        if (!listSegmentNames.contains(segmentName)) {
            log.error("Can't find list Segment, segmentName is {}.", segmentName);
            return;
        }
        segmentProxy.deleteSegmentByName(customerSpace, segmentName, false);
    }

    private ListSegmentSummary generateListSegmentSummary(String customerSpace, MetadataSegment segment,
                                                          boolean needTableInfo) {
        if (segment == null) {
            log.error("Can't find list Segment");
            return null;
        }
        ListSegment listSegment = segment.getListSegment();
        ListSegmentSummary segmentSummary = new ListSegmentSummary();
        segmentSummary.setSegmentName(segment.getName());
        segmentSummary.setCsvAdaptor(listSegment.getCsvAdaptor());
        segmentSummary.setS3UploadDropFolder(listSegment.getS3DropFolder());
        if (!needTableInfo) {
            return segmentSummary;
        }
        Map<String, String> dataTemplates = listSegment.getDataTemplates();
        if (MapUtils.isEmpty(dataTemplates) || dataTemplates.get(BusinessEntity.Account.name()) == null) {
            log.warn("Can't find dataTemplate.");
            return segmentSummary;
        }
        String dataTemplateId = dataTemplates.get(BusinessEntity.Account.name());
        DataUnit dataUnit = dataUnitProxy.getByDataTemplateIdAndRole(customerSpace, dataTemplateId,
                DataUnit.Role.Master);
        if (dataUnit == null) {
            log.warn("dataUnit is null. dataTemplateId is {}.", dataTemplateId);
            return segmentSummary;
        }
        S3DataUnit dataUnit1 = (S3DataUnit) dataUnit;
        segmentSummary.setTableName(dataUnit1.getName());
        segmentSummary.setTableLocation(dataUnit1.getLinkedDir());
        segmentSummary.setTableHdfsLocation(dataUnit1.getLinkedHdfsPath());
        return segmentSummary;
    }

    private MetadataSegment generateMetadataSegment(String displayName) {
        MetadataSegment metadataSegment = new MetadataSegment();
        metadataSegment.setDisplayName(displayName);
        ListSegment listSegment = new ListSegment();
        listSegment.setExternalSystem(null);
        listSegment.setExternalSegmentId(UuidUtil.getTimeBasedUuid().toString());
        metadataSegment.setListSegment(listSegment);
        return metadataSegment;
    }

    /*-
     * FIXME change to real implementation, currently return mock data to unblock UI
     */
    private Map<String, String> getDashboardMap(@NotNull String customerSpace) {
        String tenant = CustomerSpace.shortenCustomerSpace(customerSpace);
        String webVisitTableName = dataCollectionProxy.getTableName(customerSpace,
                TableRoleInCollection.ConsolidatedWebVisit);
        if (StringUtils.isEmpty(webVisitTableName)) {
            return null;
        }
        Map<String, Object> userAttrs = new HashMap<>();
        userAttrs.put(USER_ATTR_WEB_VISIT_DATA_TABLE, webVisitTableName);
        String accountTableName = getTargetAccountTableName(customerSpace);
        if (!StringUtils.isEmpty(accountTableName)) {
            userAttrs.put(USER_ATTR_TARGET_ACCOUNT_LIST_TABLE, accountTableName);
        }
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
            data.setUserAttributes(userAttrs);
            return Pair.of(dashboard, EmbedUrlUtils.signEmbedDashboardUrl(data));
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    private String getTargetAccountTableName(String customerSpace) {
        DataCollectionStatus status = dataCollectionProxy.getOrCreateDataCollectionStatus(customerSpace, null);
        List<String> segmentNames = status.getListSegmentNames();
        if (CollectionUtils.isEmpty(segmentNames)) {
            return null;
        }
        String segmentNanme = segmentNames.get(segmentNames.size() - 1);
        ListSegmentSummary segmentSummary = getListSegmentSummary(customerSpace, segmentNanme);
        return segmentSummary.getTableName();
    }
}
