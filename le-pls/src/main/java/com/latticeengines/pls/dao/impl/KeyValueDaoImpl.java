package com.latticeengines.pls.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.KeyValue;
import com.latticeengines.pls.dao.KeyValueDao;

@Component("keyValueDao")
public class KeyValueDaoImpl extends BaseDaoImpl<KeyValue> implements KeyValueDao {

    @Override
    protected Class<KeyValue> getEntityClass() {
        return KeyValue.class;
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public KeyValue findByEntityId(Long entityPid) {
        Session session = getSessionFactory().getCurrentSession();
        Class<KeyValue> entityClz = getEntityClass();
        Query query = session.createQuery("from " + entityClz.getSimpleName() + " where entityId = " + entityPid);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (KeyValue) list.get(0);
    }
    

}
