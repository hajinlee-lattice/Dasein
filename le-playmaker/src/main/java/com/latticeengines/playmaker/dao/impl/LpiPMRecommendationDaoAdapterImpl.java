package com.latticeengines.playmaker.dao.impl;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseGenericDaoImpl;
import com.latticeengines.playmaker.dao.LpiPMAccountExtensionDao;
import com.latticeengines.playmaker.dao.LpiPMPlayDao;
import com.latticeengines.playmaker.dao.LpiPMRecommendationDao;
import com.latticeengines.playmaker.dao.PlaymakerRecommendationDao;

@Component("lpiPMRecommendationDaoAdapter")
public class LpiPMRecommendationDaoAdapterImpl
        extends BaseGenericDaoImpl //
        implements PlaymakerRecommendationDao {

    @Autowired
    private LpiPMPlayDao playDao;

    @Autowired
    private LpiPMRecommendationDao recommendationDao;

    @Autowired
    private LpiPMAccountExtensionDao accountExtensionDao;

    public LpiPMRecommendationDaoAdapterImpl() {
        super(null);
    }

    public LpiPMRecommendationDaoAdapterImpl(NamedParameterJdbcTemplate namedJdbcTemplate) {
        super(namedJdbcTemplate);
    }

    @Override
    public List<Map<String, Object>> getRecommendations(long start, int offset, int maximum, int syncDestination,
            List<String> playIds) {
        return recommendationDao.getRecommendations(start, offset, maximum, syncDestination, playIds);
    }

    @Override
    public int getRecommendationCount(long start, int syncDestination, List<String> playIds) {
        return recommendationDao.getRecommendationCount(start, syncDestination, playIds);
    }

    @Override
    public List<Map<String, Object>> getPlays(long start, int offset, int maximum, List<Integer> playgroupIds) {
        return playDao.getPlays(start, offset, maximum, playgroupIds);
    }

    @Override
    public int getPlayCount(long start, List<Integer> playgroupIds) {
        return playDao.getPlayCount(start, playgroupIds);
    }

    @Override
    public List<Map<String, Object>> getAccountExtensions(long start, int offset, int maximum, List<String> accountIds,
            String filterBy, Long recStart, String columns, boolean hasSfdcContactId) {
        return accountExtensionDao.getAccountExtensions(start, offset, maximum, accountIds, recStart, columns,
                hasSfdcContactId);
    }

    @Override
    public int getAccountExtensionCount(long start, List<String> accountIds, String filterBy, Long recStart) {
        return accountExtensionDao.getAccountExtensionCount(start, accountIds, recStart);
    }

    @Override
    public List<Map<String, Object>> getAccountExtensionSchema() {
        throw new NotImplementedException();
    }

    @Override
    public int getAccountExtensionColumnCount() {
        throw new NotImplementedException();
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
        throw new NotImplementedException();
    }

    @Override
    public int getContactExtensionColumnCount() {
        throw new NotImplementedException();
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
        throw new NotImplementedException();
    }

    @Override
    public List<Map<String, Object>> getPlayGroups(long start, int offset, int maximum) {
        throw new NotImplementedException();
    }

    @Override
    public int getPlayGroupCount(long start) {
        throw new NotImplementedException();
    }

    @Override
    public List<Map<String, Object>> queryForListOfMap(String sql, MapSqlParameterSource parameters) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T> T queryForObject(String sql, MapSqlParameterSource parameters, Class<T> requiredType) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void update(String sql, MapSqlParameterSource parameters) {
        // TODO Auto-generated method stub

    }
}
