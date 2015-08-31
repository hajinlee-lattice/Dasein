package com.latticeengines.upgrade.jdbc;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.upgrade.yarn.YarnPathUtils;

@Component
public class PlsMultiTenantJdbcManager {

    @Autowired
    @Qualifier("plsJdbcTemlate")
    private JdbcTemplate plsJdbcTemlate;

    private static final String TENANT_TABLE = "TENANT";
    private static final String MODEL_SUMMARY_TABLE = "MODEL_SUMMARY";
    private static Set<String> uuids;

    @PostConstruct
    public void getModelGuids() {
        List<String> ids = plsJdbcTemlate.queryForList("SELECT ID FROM " + MODEL_SUMMARY_TABLE, String.class);
        uuids = new HashSet<>();
        for (String id: ids) {
            try {
                uuids.add(YarnPathUtils.extractUuid(id));
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
    }

    public Set<String> getModelGuidsForCustomer(String tenantName) {
        List<String> ids = plsJdbcTemlate.queryForList("SELECT ID FROM " + MODEL_SUMMARY_TABLE
                + " lhs INNER JOIN " + TENANT_TABLE + " rhs ON lhs.[FK_TENANT_ID] = rhs.[TENANT_PID] "
                + " AND rhs.[NAME] = \'" + tenantName + "\'", String.class);
        Set<String> toReturn = new HashSet<>();
        for (String id: ids) {
            toReturn.add(YarnPathUtils.extractUuid(id));
        }
        return toReturn;
    }

    public boolean hasUuid(String modelGuidOrUuid) {
        return uuids.contains(YarnPathUtils.extractUuid(modelGuidOrUuid));
    }

    public String findNameByUuid(String uuid) {
        List<String> names =  plsJdbcTemlate.queryForList("SELECT NAME FROM " + MODEL_SUMMARY_TABLE
                        + " WHERE ID LIKE \'%" + uuid + "%\' ORDER BY ID", String.class);
        for (String name: names) {
            if (isCustomizedName(name)) return name;
        }
        return null;
    }

    public String findModelGuidByUuid(String uuid) {
        List<String> ids =  plsJdbcTemlate.queryForList("SELECT ID FROM " + MODEL_SUMMARY_TABLE
                + " WHERE ID LIKE \'%" + uuid + "%\' ORDER BY ID", String.class);
        if (ids.isEmpty()) return null;
        return ids.get(0);
    }

    public boolean isActive(String modelGuid) {
        try {
            Integer status = plsJdbcTemlate.queryForObject("SELECT Status FROM " + MODEL_SUMMARY_TABLE
                    + " WHERE ID = \'" + modelGuid + "\'", Integer.class);
            return ModelSummaryStatus.ACTIVE.getStatusId() == status;
        } catch (EmptyResultDataAccessException e) {
            return false;
        }
    }

    public void deleteModelSummariesByUuid(String modelId) {
        String uuid = YarnPathUtils.extractUuid(modelId);
        plsJdbcTemlate.execute("DELETE FROM " + MODEL_SUMMARY_TABLE + " WHERE ID LIKE \'%" + uuid + "%\'");
    }

    public void deleteModelSummariesByTenantId(String tenantId) {
        plsJdbcTemlate.execute("DELETE summary " +
                "FROM " + MODEL_SUMMARY_TABLE + " AS summary " +
                "INNER JOIN " + TENANT_TABLE + " AS tenant " +
                "ON summary.FK_TENANT_ID = tenant.TENANT_PID " +
                "WHERE tenant.TENANT_ID = \'" + tenantId + "\'");
    }

    public void substituteSingularIdByTupleId(String customer) {
        String singularId = CustomerSpace.parse(customer).getTenantId();
        String tupleId = CustomerSpace.parse(customer).toString();
        plsJdbcTemlate.execute("UPDATE " + TENANT_TABLE
                + " SET TENANT_ID = \'" + tupleId + "\'"
                + " WHERE TENANT_ID = \'" + singularId + "\'");
    }

    public boolean hasTenantId(String tenantId) {
        try {
            plsJdbcTemlate.queryForObject("SELECT TENANT_ID FROM " + TENANT_TABLE
                    + " WHERE TENANT_ID = \'" + tenantId + "\'", String.class);
            return true;
        } catch (EmptyResultDataAccessException e) {
            return false;
        }
    }

    private boolean isCustomizedName(String name) {
        return !(name.startsWith("PLSModel-") || name.endsWith(" GMT"));
    }
}
