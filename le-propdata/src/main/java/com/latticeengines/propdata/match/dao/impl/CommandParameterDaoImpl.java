package com.latticeengines.propdata.match.dao.impl;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

import org.hibernate.internal.SessionImpl;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.CommandParameter;
import com.latticeengines.propdata.match.dao.CommandParameterDao;

@Component("commandParameterDao")
public class CommandParameterDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<CommandParameter>
        implements CommandParameterDao {

    @Override
    protected Class<CommandParameter> getEntityClass() {
        return CommandParameter.class;
    }

    @Override
    public void registerParameter(String uid, String key, String value) {
        try {
            Connection conn = ((SessionImpl) sessionFactory.getCurrentSession()).connection();
            CallableStatement cstmt = conn.prepareCall("{call dbo.MatchFramework_RegisterCommandParameter(?, ?, ?, ?)}");
            cstmt.setString("uid", uid);
            cstmt.setString("key", key);
            cstmt.setString("value", value);
            cstmt.setBoolean("isInferred", false);
            cstmt.execute();
            cstmt.close();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register command parameters by stored procedure.", e);
        }
    }

}
