package com.latticeengines.upgrade.jdbc;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

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
            uuids.add(YarnPathUtils.extractUuid(id));
        }
    }

    public boolean hasUuid(String modelGuidOrUuid) {
        return uuids.contains(YarnPathUtils.extractUuid(modelGuidOrUuid));
    }

    public String findNameByUuid(String uuid) {
        List<String> names =  plsJdbcTemlate.queryForList("SELECT NAME FROM " + MODEL_SUMMARY_TABLE
                        + " WHERE ID LIKE \'%" + uuid + "%\' ORDER BY LENGTH(ID)", String.class);
        for (String name: names) {
            if (isCustomizedName(name)) return name;
        }
        return null;
    }

    public String findModelGuidByUuid(String uuid) {
        List<String> ids =  plsJdbcTemlate.queryForList("SELECT ID FROM " + MODEL_SUMMARY_TABLE
                + " WHERE ID LIKE \'%" + uuid + "%\' ORDER BY LENGTH(ID)", String.class);
        if (ids.isEmpty()) return null;
        return ids.get(0);
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

    private boolean isCustomizedName(String name) {
        return !(name.startsWith("PLSModel-") || name.endsWith(" GMT"));
    }
}
