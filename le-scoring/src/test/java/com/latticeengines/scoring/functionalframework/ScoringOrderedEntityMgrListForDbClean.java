package com.latticeengines.scoring.functionalframework;

import java.util.List;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.ImmutableList;
import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandLogEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandStateEntityMgr;

@Configuration
public class ScoringOrderedEntityMgrListForDbClean {

    @Inject
    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    @Inject
    private ScoringCommandStateEntityMgr scoringCommandStateEntityMgr;

    @Inject
    private ScoringCommandLogEntityMgr scoringCommandLogEntityMgr;

    @Inject
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    private List<BaseEntityMgr<? extends HasPid>> entityMgrs;

    @PostConstruct
    public void init() {
        entityMgrs = ImmutableList.of(scoringCommandLogEntityMgr, scoringCommandStateEntityMgr,
                scoringCommandEntityMgr, scoringCommandResultEntityMgr);
    }

    @Bean
    public List<BaseEntityMgr<? extends HasPid>> entityMgrs() {
        return entityMgrs;
    }
}
