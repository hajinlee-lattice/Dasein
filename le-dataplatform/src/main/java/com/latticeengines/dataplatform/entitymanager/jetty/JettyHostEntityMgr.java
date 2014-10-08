package com.latticeengines.dataplatform.entitymanager.jetty;

import java.util.List;

import com.latticeengines.dataplatform.entitymanager.BaseEntityMgr;
import com.latticeengines.domain.exposed.jetty.JettyHost;
import com.latticeengines.domain.exposed.jetty.JettyJob;

public interface JettyHostEntityMgr extends BaseEntityMgr<JettyHost> {

    List<JettyHost> findByJettyJob(JettyJob jettyJob);

}
