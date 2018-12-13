package com.latticeengines.objectapi.service;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.util.TimeFilterTranslator;

public interface TransactionService {

    String getMaxTransactionDate(DataCollection.Version version);

    TimeFilterTranslator getTimeFilterTranslator(String evaluationDate);

    boolean needTimeFilterTranslator(FrontEndQuery frontEndQuery);

}
