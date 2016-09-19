package com.latticeengines.pls.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.Enrichment;
import com.latticeengines.domain.exposed.pls.MarketoMatchField;
import com.latticeengines.domain.exposed.pls.MarketoMatchFieldName;

public interface MarketoMatchFieldDao extends BaseDao<MarketoMatchField> {

    void deleteMarketoMatchField(MarketoMatchFieldName fieldName, Enrichment enrichment);

}
