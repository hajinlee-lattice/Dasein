package com.latticeengines.dataplatform.dao.jetty;

import java.util.List;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.domain.exposed.jetty.JettyHost;
import com.latticeengines.domain.exposed.jetty.JettyJob;

public interface JettyHostDao extends BaseDao<JettyHost> {

    List<JettyHost> findByJettyJob(JettyJob jettyJob);

}
