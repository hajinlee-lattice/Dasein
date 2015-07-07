package com.latticeengines.upgrade.jdbc;

import java.util.ArrayList;
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

    private static final String MODEL_SUMMARY_TABLE = "MODEL_SUMMARY";
    private static Set<String> modelIds;

    @PostConstruct
    private void getModelGuids() {
        List<String> ids = plsJdbcTemlate.queryForList("SELECT ID FROM " + MODEL_SUMMARY_TABLE, String.class);
        List<String> guids = new ArrayList<>();
        for (String id: ids) {
            guids.add(YarnPathUtils.extractUuid(id));
        }
        modelIds = new HashSet<>(guids);
    }

    public boolean hasUuid(String modelGuidOrUuid) {
        return modelIds.contains(YarnPathUtils.extractUuid(modelGuidOrUuid));
    }

    public String findNameByUuid(String uuid) {
        String modelId = YarnPathUtils.constructModelGuidFromUuid(uuid);
        return plsJdbcTemlate.queryForObject(
                "SELECT NAME FROM " + MODEL_SUMMARY_TABLE + " WHERE ID = \'" + modelId + "\'", String.class);
    }

    public void deleteByUuid(String modelId) {
        String uuid = YarnPathUtils.extractUuid(modelId);
        plsJdbcTemlate.execute("DELETE FROM " + MODEL_SUMMARY_TABLE + " WHERE ID LIKE \'%" + uuid + "%\'");
    }
}
