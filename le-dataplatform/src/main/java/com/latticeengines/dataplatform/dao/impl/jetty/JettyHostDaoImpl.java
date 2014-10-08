package com.latticeengines.dataplatform.dao.impl.jetty;

import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;

import com.latticeengines.dataplatform.dao.impl.BaseDaoImpl;
import com.latticeengines.dataplatform.dao.jetty.JettyHostDao;
import com.latticeengines.domain.exposed.jetty.JettyHost;
import com.latticeengines.domain.exposed.jetty.JettyJob;

public class JettyHostDaoImpl extends BaseDaoImpl<JettyHost> implements JettyHostDao {

    public JettyHostDaoImpl() {
        super();
    }

    @Override
    protected Class<JettyHost> getEntityClass() {
        return JettyHost.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<JettyHost> findByJettyJob(JettyJob jettyJob) {
        Session session = getSessionFactory().getCurrentSession();
        List<JettyHost> jettyHosts = session.createCriteria(JettyHost.class).add(Restrictions.eq("jettyJob", jettyJob)).setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY).list();
        return jettyHosts;
    }

}
