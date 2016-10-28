package com.latticeengines.modelquality.dao.impl;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelquality.AnalyticTest;
import com.latticeengines.modelquality.dao.AnalyticTestDao;

@Component("qualityAnalyticTestDao")
public class AnalyticTestDaoImpl extends BaseDaoImpl<AnalyticTest> implements AnalyticTestDao {

    @Override
    protected Class<AnalyticTest> getEntityClass() {
        return AnalyticTest.class;
    }
    
    @Override
    public void deleteAll() {
        Session session = getSessionFactory().getCurrentSession();
        
        // Need to delete Many-To-Many associations first with native sql and then delete all.
        
        Class<AnalyticTest> entityClz = getEntityClass();
        Query querytoDeleteAPAssociations = session.createSQLQuery("delete from MODELQUALITY_AP_TEST_AP_PIPELINE");
        querytoDeleteAPAssociations.executeUpdate();
        
        Query querytoDeleteDSAssociations = session.createSQLQuery("delete from MODELQUALITY_AP_TEST_DATASET");
        querytoDeleteDSAssociations.executeUpdate();
        
        Query query = session.createQuery("delete from " + entityClz.getSimpleName());
        query.executeUpdate();
    }

}
