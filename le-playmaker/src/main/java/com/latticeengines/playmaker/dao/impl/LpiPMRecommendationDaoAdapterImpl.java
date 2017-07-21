package com.latticeengines.playmaker.dao.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseGenericDaoImpl;
import com.latticeengines.domain.exposed.playmakercore.SynchronizationDestinationEnum;
import com.latticeengines.playmaker.dao.PlaymakerRecommendationDao;
import com.latticeengines.playmaker.service.LpiPMAccountExtension;
import com.latticeengines.playmaker.service.LpiPMPlay;
import com.latticeengines.playmaker.service.LpiPMRecommendation;

@Component("lpiPMRecommendationDaoAdapter")
public class LpiPMRecommendationDaoAdapterImpl extends BaseGenericDaoImpl implements PlaymakerRecommendationDao {

    @Autowired
    private LpiPMPlay lpiPMPlay;

    @Autowired
    private LpiPMRecommendation lpiPMRecommendation;

    @Autowired
    private LpiPMAccountExtension lpiPMAccountExtension;

    public LpiPMRecommendationDaoAdapterImpl() {
        super(null);
    }

    public LpiPMRecommendationDaoAdapterImpl(NamedParameterJdbcTemplate namedJdbcTemplate) {
        super(namedJdbcTemplate);
    }

    @Override
    public List<Map<String, Object>> getRecommendations(long start, int offset, int maximum, int syncDestination,
            List<String> playIds) {
        SynchronizationDestinationEnum syncDestEnum = SynchronizationDestinationEnum.fromIntValue(syncDestination);
        return lpiPMRecommendation.getRecommendations(start, offset, maximum, syncDestEnum, playIds);
    }

    @Override
    public int getRecommendationCount(long start, int syncDestination, List<String> playIds) {
        SynchronizationDestinationEnum syncDestEnum = SynchronizationDestinationEnum.fromIntValue(syncDestination);
        return lpiPMRecommendation.getRecommendationCount(start, syncDestEnum, playIds);
    }

    @Override
    public List<Map<String, Object>> getPlays(long start, int offset, int maximum, List<Integer> playgroupIds) {
        return lpiPMPlay.getPlays(start, offset, maximum, playgroupIds);
    }

    @Override
    public int getPlayCount(long start, List<Integer> playgroupIds) {
        return lpiPMPlay.getPlayCount(start, playgroupIds);
    }

    @Override
    public List<Map<String, Object>> getAccountExtensions(long start, int offset, int maximum, List<String> accountIds,
            String filterBy, Long recStart, String columns, boolean hasSfdcContactId) {
        return lpiPMAccountExtension.getAccountExtensions(start, offset, maximum, accountIds, recStart, columns,
                hasSfdcContactId);
    }

    @Override
    public int getAccountExtensionCount(long start, List<String> accountIds, String filterBy, Long recStart) {
        return lpiPMAccountExtension.getAccountExtensionCount(start, accountIds, recStart);
    }

    @Override
    public List<Map<String, Object>> getAccountExtensionSchema() {
        return lpiPMAccountExtension.getAccountExtensionSchema();
    }

    @Override
    public int getAccountExtensionColumnCount() {
        return lpiPMAccountExtension.getAccountExtensionColumnCount();
    }

    @Override
    public List<Map<String, Object>> getContacts(long start, int offset, int maximum, List<Integer> contactIds,
            List<Integer> accountIds) {
        throw new NotImplementedException();
    }

    @Override
    public int getContactCount(long start, List<Integer> contactIds, List<Integer> accountIds) {
        throw new NotImplementedException();
    }

    @Override
    public List<Map<String, Object>> getContactExtensions(long start, int offset, int maximum,
            List<Integer> contactIds) {
        throw new NotImplementedException();
    }

    @Override
    public int getContactExtensionCount(long start, List<Integer> contactIds) {
        throw new NotImplementedException();
    }

    @Override
    public List<Map<String, Object>> getContactExtensionSchema() {
        return lpiPMAccountExtension.getContactExtensionSchema();
    }

    @Override
    public int getContactExtensionColumnCount() {
        return lpiPMAccountExtension.getContactExtensionColumnCount();
    }

    @Override
    public List<Map<String, Object>> getPlayValues(long start, int offset, int maximum, List<Integer> playgroupIds) {
        throw new NotImplementedException();
    }

    @Override
    public int getPlayValueCount(long start, List<Integer> playgroupIds) {
        throw new NotImplementedException();
    }

    @Override
    public List<Map<String, Object>> getWorkflowTypes() {
        // TODO - dummy impl in M13, fix in M14
        List<Map<String, Object>> result = new ArrayList<>();
        Map<String, Object> wf1 = new HashMap<>();
        wf1.put("ID", "WfTypeId1");
        wf1.put("DisplayName", "Workflow Type 1");
        result.add(wf1);

        return result;
    }

    @Override
    public List<Map<String, Object>> getPlayGroups(long start, int offset, int maximum) {
        // TODO - dummy impl in M13, fix in M14
        List<Map<String, Object>> result = new ArrayList<>();
        Map<String, Object> pg1 = new HashMap<>();
        pg1.put("ID", "PG1");
        pg1.put("ExternalID", "PG1");
        pg1.put("DisplayName", "Group 1");
        pg1.put("LastModificationDate", 0);
        result.add(pg1);
        Map<String, Object> pg2 = new HashMap<>();
        pg1.put("ID", "PG2");
        pg1.put("ExternalID", "PG2");
        pg1.put("DisplayName", "Group 2");
        pg1.put("LastModificationDate", 0);
        result.add(pg2);

        return result;
    }

    @Override
    public int getPlayGroupCount(long start) {
        // TODO - dummy impl in M13, fix in M14
        return 2;
    }

    @Override
    public List<Map<String, Object>> queryForListOfMap(String sql, MapSqlParameterSource parameters) {
        throw new NotImplementedException();
    }

    @Override
    public <T> T queryForObject(String sql, MapSqlParameterSource parameters, Class<T> requiredType) {
        throw new NotImplementedException();
    }

    @Override
    public void update(String sql, MapSqlParameterSource parameters) {
        throw new NotImplementedException();
    }
}
