package com.latticeengines.playmaker.dao.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseGenericDaoImpl;
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.playmakercore.SynchronizationDestinationEnum;
import com.latticeengines.playmaker.dao.PlaymakerRecommendationDao;
import com.latticeengines.playmaker.service.LpiPMAccountExtension;
import com.latticeengines.playmaker.service.LpiPMPlay;
import com.latticeengines.playmakercore.dao.impl.RecommendationDaoImpl;
import com.latticeengines.playmakercore.entitymanager.RecommendationEntityMgr;
import com.latticeengines.playmakercore.service.LpiPMRecommendation;

@Component("lpiPMRecommendationDaoAdapter")
public class LpiPMRecommendationDaoAdapterImpl extends BaseGenericDaoImpl implements PlaymakerRecommendationDao {
    
    private static final Logger log = LoggerFactory.getLogger(LpiPMRecommendationDaoAdapterImpl.class);

    private static final String ACC_EXT_LAST_MODIFIED_FIELD_NAME = "LastModified";

    private static final String ELOQUA_APP_ID = "lattice.eloqua";

    @Inject
    private LpiPMPlay lpiPMPlay;

    @Inject
    private LpiPMRecommendation lpiPMRecommendation;

    @Inject
    private LpiPMAccountExtension lpiPMAccountExtension;

    @Inject
    private RecommendationEntityMgr recommendationEntityMgr;

    public LpiPMRecommendationDaoAdapterImpl() {
        super(null);
    }

    public LpiPMRecommendationDaoAdapterImpl(NamedParameterJdbcTemplate namedJdbcTemplate) {
        super(namedJdbcTemplate);
    }

    @Override
    public List<Map<String, Object>> getRecommendations(long start, int offset, int maximum, int syncDestination,
            List<String> playIds, Map<String, String> orgInfo, Map<String, String> appId) {
        boolean latestLaunchFlag = false;
        if (appId != null) {
            if (appId.get(CDLConstants.AUTH_APP_ID).startsWith(ELOQUA_APP_ID)) {
                latestLaunchFlag = true;
            }
        }
        List<String> launchIds = lpiPMPlay.getLaunchIdsFromDashboard(latestLaunchFlag, start, playIds, 0, orgInfo);
        if (CollectionUtils.isNotEmpty(launchIds)) {
            return lpiPMRecommendation.getRecommendationsByLaunchIds(launchIds, offset, maximum);
        }
        else {
            return null;
        }
    }

    @Override
    public long getRecommendationCount(long start, int syncDestination, List<String> playIds,
            Map<String, String> orgInfo, Map<String, String> appId) {
        boolean latestLaunchFlag = false;
        if (appId != null) {
            if (appId.get(CDLConstants.AUTH_APP_ID).startsWith(ELOQUA_APP_ID)) {
                latestLaunchFlag = true;
            }
        }
        List<String> launchIds = lpiPMPlay.getLaunchIdsFromDashboard(latestLaunchFlag, start, playIds, 0, orgInfo);
        if (CollectionUtils.isNotEmpty(launchIds)) {
            return lpiPMRecommendation.getRecommendationCountByLaunchIds(launchIds);
        }
        else {
            return 0;
        }
    }

//    @Override
//    public List<Map<String, Object>> getRecommendations(long start, int offset, int maximum, int syncDestination,
//            List<String> playIds, Map<String, String> orgInfo, Map<String, String> appId) {
//        SynchronizationDestinationEnum syncDestEnum = SynchronizationDestinationEnum.fromIntValue(syncDestination);
//        if (appId != null) {
//            if (appId.get(CDLConstants.AUTH_APP_ID).startsWith(ELOQUA_APP_ID)) {
//                List<String> launchIds = lpiPMPlay.getLaunchIdsFromDashboard(true, start, null, 0, orgInfo);
//                return lpiPMRecommendation.getRecommendationsByLaunchIds(launchIds, offset, maximum);
//            }
//        }
//        return lpiPMRecommendation.getRecommendations(start, offset, maximum, syncDestEnum, playIds, orgInfo, appId);
//    }
//
//    @Override
//    public long getRecommendationCount(long start, int syncDestination, List<String> playIds,
//            Map<String, String> orgInfo, Map<String, String> appId) {
//        SynchronizationDestinationEnum syncDestEnum = SynchronizationDestinationEnum.fromIntValue(syncDestination);
//        if (appId != null) {
//            if (appId.get(CDLConstants.AUTH_APP_ID).startsWith(ELOQUA_APP_ID)) {
//                List<String> launchIds = lpiPMPlay.getLaunchIdsFromDashboard(true, start, null, 0, orgInfo);
//                return lpiPMRecommendation.getRecommendationCountByLaunchIds(launchIds);
//            }
//        }
//        return lpiPMRecommendation.getRecommendationCount(start, syncDestEnum, playIds, orgInfo, appId);
//    }

    @Override
    public List<Map<String, Object>> getPlays(long start, int offset, int maximum, List<Integer> playgroupIds,
            int syncDestination, Map<String, String> orgInfo) {
        return lpiPMPlay.getPlays(start, offset, maximum, playgroupIds, syncDestination, orgInfo);
    }

    @Override
    public long getPlayCount(long start, List<Integer> playgroupIds, int syncDestination, Map<String, String> orgInfo) {
        return lpiPMPlay.getPlayCount(start, playgroupIds, syncDestination, orgInfo);
    }

    @Override
    public List<Map<String, Object>> getAccountExtensions(Long start, int offset, int maximum, List<String> accountIds,
            String filterBy, Long recStart, String columns, boolean hasSfdcContactId, Map<String, String> orgInfo) {

        if (StringUtils.isBlank(columns)) {
            columns = ACC_EXT_LAST_MODIFIED_FIELD_NAME;
        } else {
            columns = ACC_EXT_LAST_MODIFIED_FIELD_NAME + "," + columns;
        }

        return lpiPMAccountExtension.getAccountExtensions(start, offset, maximum, accountIds, filterBy, recStart,
                columns, hasSfdcContactId, orgInfo);
    }

    @Override
    public long getAccountExtensionCount(Long start, List<String> accountIds, String filterBy, Long recStart,
            Map<String, String> orgInfo) {
        return lpiPMAccountExtension.getAccountExtensionCount(start, accountIds, filterBy, recStart, orgInfo);
    }

    @Override
    public List<Map<String, Object>> getAccountExtensionSchema(String customerSpace) {
        return lpiPMAccountExtension.getAccountExtensionSchema(customerSpace);
    }

    @Override
    public long getAccountExtensionColumnCount(String customerSpace) {
        return lpiPMAccountExtension.getAccountExtensionColumnCount(customerSpace);
    }

    @Override
    public List<Map<String, Object>> getContacts(long start, int offset, int maximum, List<String> contactIds,
            List<String> accountIds, Long recStart, Map<String, String> orgInfo, Map<String, String> appId) {
        log.info("Atlas getContacts: " + orgInfo.toString() + "\t" + appId.toString() + "\n");
        List<Map<String, Object>> contactList = null;
        List<String> launchIds = lpiPMPlay.getLaunchIdsFromDashboard(true, start, null, 0, orgInfo);
        log.info("Atlas get accountIds: " + launchIds + "\n");
        contactList = recommendationEntityMgr.findContactsByLaunchIds(launchIds, accountIds);
        log.info("Atlas queried contacts : " + contactList + "\n");
        if (CollectionUtils.isNotEmpty(contactList)) {
            contactList = contactList.subList(Math.min(contactList.size() - 1, offset),
                    Math.min(maximum, contactList.size()));
        }
        log.info("Atlas reply contacts : " + contactList + "\n");
        return contactList;
    }

    @Override
    public long getContactCount(long start, List<String> contactIds, List<String> accountIds, Long recStart,
            Map<String, String> orgInfo, Map<String, String> appId) {
        int result = 0;
        List<String> launchIds = lpiPMPlay.getLaunchIdsFromDashboard(true, start, null, 0, orgInfo);
        result = recommendationEntityMgr.findContactsCountByLaunchIds(launchIds, accountIds);
        return result;
    }

    @Override
    public List<Map<String, Object>> getContactExtensions(long start, int offset, int maximum, List<String> contactIds,
            Long recStart, Map<String, String> orgInfo, Map<String, String> appId) {
        // TODO - not implemented in M13
        return new ArrayList<>();
    }

    @Override
    public long getContactExtensionCount(long start, List<String> contactIds, Long recStart,
            Map<String, String> orgInfo, Map<String, String> appId) {
        // TODO - not implemented in M13
        return 0L;
    }

    @Override
    public List<Map<String, Object>> getContactExtensionSchema(String customerSpace) {
        return lpiPMAccountExtension.getContactExtensionSchema(customerSpace);
    }

    @Override
    public long getContactExtensionColumnCount(String customerSpace) {
        return lpiPMAccountExtension.getContactExtensionColumnCount(customerSpace);
    }

    @Override
    public List<Map<String, Object>> getPlayValues(long start, int offset, int maximum, List<Integer> playgroupIds) {
        // TODO - not implemented in M13
        return new ArrayList<>();
    }

    @Override
    public long getPlayValueCount(long start, List<Integer> playgroupIds) {
        // TODO - not implemented in M13
        return 0;
    }

    @Override
    public List<Map<String, Object>> getWorkflowTypes() {
        // TODO - dummy impl in M13, fix in M14
        List<Map<String, Object>> result = new ArrayList<>();

        createWorkflowTypeMap(result, "ADefault", "List");
        createWorkflowTypeMap(result, "Cross-Sell", "Cross-Sell");
        createWorkflowTypeMap(result, "Prospecting", "Prospecting");
        createWorkflowTypeMap(result, "Renewal", "Renewal");
        createWorkflowTypeMap(result, "Upsell", "Upsell");

        return result;
    }

    private void createWorkflowTypeMap(List<Map<String, Object>> result, String type, String typeDisplayName) {
        Map<String, Object> wf = new HashMap<>();
        wf.put("ID", type);
        wf.put("DisplayName", typeDisplayName);
        result.add(wf);
    }

    @Override
    public List<Map<String, Object>> getPlayGroups(long start, int offset, int maximum) {
        // TODO - dummy impl in M13, fix in M14
        List<Map<String, Object>> result = new ArrayList<>();

        createPlayGroupMap(result, 1, "Enterprise", "Enterprise", 0, 1);
        createPlayGroupMap(result, 2, "Marketing", "Marketing", 0, 2);
        createPlayGroupMap(result, 3, "PlayGroup001", "Play Group 1", 0, 3);
        createPlayGroupMap(result, 4, "PlayGroup002", "Play Group 2", 0, 4);
        createPlayGroupMap(result, 5, "Renewals", "Renewals", 0, 5);

        return result;
    }

    private void createPlayGroupMap(List<Map<String, Object>> result, int id, String externalId, String displayName,
            int lastModificationDate, int rowNum) {
        Map<String, Object> pg = new HashMap<>();
        pg.put("ID", id);
        pg.put("ExternalID", externalId);
        pg.put("DisplayName", displayName);
        pg.put("LastModificationDate", lastModificationDate);
        pg.put("RowNum", rowNum);
        result.add(pg);
    }

    @Override
    public long getPlayGroupCount(long start) {
        // TODO - dummy impl in M13, fix in M14
        return 2;
    }

    @Override
    public List<Map<String, Object>> queryForListOfMap(String sql, MapSqlParameterSource parameters) {
        throw new NotImplementedException("Not implemented.");
    }

    @Override
    public <T> T queryForObject(String sql, MapSqlParameterSource parameters, Class<T> requiredType) {
        throw new NotImplementedException("Not implemented.");
    }

    @Override
    public void update(String sql, MapSqlParameterSource parameters) {
        throw new NotImplementedException("Not implemented.");
    }
}
