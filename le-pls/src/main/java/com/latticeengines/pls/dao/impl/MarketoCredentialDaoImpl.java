package com.latticeengines.pls.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.MarketoCredential;
import com.latticeengines.pls.dao.MarketoCredentialDao;

@Component("marketoCredentialDao")
public class MarketoCredentialDaoImpl extends BaseDaoImpl<MarketoCredential>
        implements MarketoCredentialDao {

    @Override
    protected Class<MarketoCredential> getEntityClass() {
        return MarketoCredential.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public MarketoCredential findMarketoCredentialById(String credentialId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<MarketoCredential> entityClz = getEntityClass();
        String queryStr = String.format("from %s where pid = :credentialId",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("credentialId", credentialId);
        List<MarketoCredential> marketoCredentials = query.list();
        if (marketoCredentials.size() == 0) {
            return null;
        }
        return marketoCredentials.get(0);
    }

    @Override
    public void deleteMarketoCredentialById(String credentialId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<MarketoCredential> entityClz = getEntityClass();
        String queryStr = String.format("delete from %s where pid = :credentialId",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("credentialId", credentialId);
        query.executeUpdate();
    }

}
