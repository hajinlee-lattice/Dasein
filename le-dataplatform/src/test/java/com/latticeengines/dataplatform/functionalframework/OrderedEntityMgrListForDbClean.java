package com.latticeengines.dataplatform.functionalframework;

import java.util.List;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.ImmutableList;
import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandLogEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandParameterEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandResultEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.dataplatform.entitymanager.modeling.AlgorithmEntityMgr;
import com.latticeengines.dataplatform.entitymanager.modeling.ModelDefinitionEntityMgr;
import com.latticeengines.dataplatform.entitymanager.modeling.ModelEntityMgr;
import com.latticeengines.dataplatform.entitymanager.modeling.ThrottleConfigurationEntityMgr;
import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.yarn.exposed.entitymanager.JobEntityMgr;

@Configuration
public class OrderedEntityMgrListForDbClean {

    @Inject
    protected AlgorithmEntityMgr algorithmEntityMgr;

    @Inject
    protected ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr;

    @Inject
    protected JobEntityMgr jobEntityMgr;

    @Inject
    protected ModelEntityMgr modelEntityMgr;

    @Inject
    protected ModelDefinitionEntityMgr modelDefinitionEntityMgr;

    @Inject
    private ModelCommandLogEntityMgr modelCommandLogEntityMgr;

    @Inject
    private ModelCommandStateEntityMgr modelCommandStateEntityMgr;

    @Inject
    private ModelCommandParameterEntityMgr modelCommandParameterEntityMgr;

    @Inject
    private ModelCommandResultEntityMgr modelCommandResultEntityMgr;

    @Inject
    private ModelCommandEntityMgr modelCommandEntityMgr;

    private List<BaseEntityMgr<? extends HasPid>> entityMgrs;

    @PostConstruct
    public void postConstruct() {
        entityMgrs = ImmutableList.of(algorithmEntityMgr, throttleConfigurationEntityMgr, jobEntityMgr, modelEntityMgr,
                modelDefinitionEntityMgr, modelCommandLogEntityMgr, modelCommandStateEntityMgr,
                modelCommandParameterEntityMgr, modelCommandResultEntityMgr, modelCommandEntityMgr);
    }

    @Bean
    public List<BaseEntityMgr<? extends HasPid>> entityMgrs() {
        return entityMgrs;
    }
}
