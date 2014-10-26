package com.latticeengines.propdata.service.db;

import com.latticeengines.propdata.service.db.PropDataContext;

public interface PropDataDBService {

    PropDataContext exportToDB(PropDataContext requestContext);

    PropDataContext importFromDB(PropDataContext requestContext);

    PropDataContext addCommandAndWaitForComplete(PropDataContext requestContext);

    void createSingleTableFromAvro(PropDataContext requestContext) throws Exception;

    PropDataContext createSingleAVROFromTable(PropDataContext requestContext);

}
