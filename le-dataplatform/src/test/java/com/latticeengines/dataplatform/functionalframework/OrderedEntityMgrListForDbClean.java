package com.latticeengines.dataplatform.functionalframework;

import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.ImmutableList;
import com.latticeengines.dataplatform.entitymanager.AlgorithmEntityMgr;
import com.latticeengines.dataplatform.entitymanager.BaseEntityMgr;
import com.latticeengines.dataplatform.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandLogEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandParameterEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandResultEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelDefinitionEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ThrottleConfigurationEntityMgr;
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
