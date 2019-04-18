package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.pls.MarketoScoringMatchField;

public interface MarketoScoringMatchFieldEntityMgr extends BaseEntityMgrRepository<MarketoScoringMatchField, Long> {

    Integer deleteFields(List<MarketoScoringMatchField> fields);

}
