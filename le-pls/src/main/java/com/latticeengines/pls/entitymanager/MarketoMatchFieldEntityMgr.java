package com.latticeengines.pls.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.Enrichment;
import com.latticeengines.domain.exposed.pls.MarketoMatchField;
import com.latticeengines.domain.exposed.pls.MarketoMatchFieldName;

public interface MarketoMatchFieldEntityMgr extends BaseEntityMgr<MarketoMatchField> {

    MarketoMatchField createMatchFieldWithNameValueAndEnrichment(
            MarketoMatchFieldName marketoMatchFieldName, String marketoValue,
            Enrichment enrichment);

    void updateMarketoMatchFieldValue(MarketoMatchFieldName matchFieldName, String marketoValue,
            Enrichment enrichment);

}
