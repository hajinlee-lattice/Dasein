package com.latticeengines.upgrade.jdbc;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

@Component
public class GaJdbcManager {

    @Autowired
    @Qualifier("gaJdbcTemlate")
    private JdbcTemplate gaJdbcTemlate;

    private static final String TENANT_TABLE = "GlobalTenant";

    public String getTenantPid(String deploymentId) {
        try {
            return gaJdbcTemlate.queryForObject("SELECT GlobalTenant_ID FROM " + TENANT_TABLE
                    + " WHERE Deployment_ID = \'" + deploymentId + "\'", String.class);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    public boolean hasDeploymentId(String deploymentId) {
        return getTenantPid(deploymentId) != null;
    }

    public void substituteSingularIdByTupleId(String customer) {
        String singularId = CustomerSpace.parse(customer).getTenantId();
        String tupleId = CustomerSpace.parse(customer).toString();
        gaJdbcTemlate.execute("UPDATE " + TENANT_TABLE
                + " SET Deployment_ID = \'" + tupleId + "\'"
                + " WHERE Deployment_ID = \'" + singularId + "\'");
    }

}
