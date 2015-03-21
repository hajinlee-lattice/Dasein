package com.latticeengines.madison.service;

import com.latticeengines.propdata.service.db.PropDataContext;

public interface PropDataMadisonService {

    public static final String TODAY_KEY = "today";
    public static final String RECORD_KEY = "record";

    PropDataContext importFromDB(PropDataContext requestContext);

    PropDataContext transform(PropDataContext requestContext);

    PropDataContext exportToDB(PropDataContext requestContext);

}
