package com.latticeengines.dataplatform.functionalframework;

import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
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
import com.latticeengines.yarn.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Configuration
public class OrderedEntityMgrListForDbClean {

    @Autowired
    protected AlgorithmEntityMgr algorithmEntityMgr;

    @Autowired
    protected ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr;

    @Autowired
    protected JobEntityMgr jobEntityMgr;

    @Autowired
    protected ModelEntityMgr modelEntityMgr;

    @Autowired
    protected ModelDefinitionEntityMgr modelDefinitionEntityMgr;

    @Autowired
    private ModelCommandLogEntityMgr modelCommandLogEntityMgr;

    @Autowired
    private ModelCommandStateEntityMgr modelCommandStateEntityMgr;

    @Autowired
    private ModelCommandParameterEntityMgr modelCommandParameterEntityMgr;

    @Autowired
    private ModelCommandResultEntityMgr modelCommandResultEntityMgr;

    @Autowired
    private ModelCommandEntityMgr modelCommandEntityMgr;

    private List<BaseEntityMgr<? extends HasPid>> entityMgrs;

    @PostConstruct
    public void init() {
        entityMgrs = ImmutableList.of(algorithmEntityMgr, throttleConfigurationEntityMgr, jobEntityMgr, modelEntityMgr,
                modelDefinitionEntityMgr, modelCommandLogEntityMgr, modelCommandStateEntityMgr,
                modelCommandParameterEntityMgr, modelCommandResultEntityMgr, modelCommandEntityMgr);
    }

    @Bean
    public List<BaseEntityMgr<? extends HasPid>> entityMgrs() {
        return entityMgrs;
    }
}
