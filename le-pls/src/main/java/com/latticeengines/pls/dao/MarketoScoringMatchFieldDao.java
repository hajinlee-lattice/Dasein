package com.latticeengines.pls.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.MarketoScoringMatchField;

public interface MarketoScoringMatchFieldDao extends BaseDao<MarketoScoringMatchField> {

    Integer deleteFields(List<MarketoScoringMatchField> fields);

}
