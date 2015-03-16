package com.latticeengines.madison.service;

import com.latticeengines.propdata.service.db.PropDataContext;

public interface PropDataMadisonService {

    PropDataContext importFromDB(PropDataContext requestContext);

    PropDataContext transform(PropDataContext requestContext);

    PropDataContext exportToDB(PropDataContext requestContext);

}
