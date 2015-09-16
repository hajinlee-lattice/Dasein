package com.latticeengines.propdata.eai.service;



public interface PropDataDBService {

    PropDataContext exportToDB(PropDataContext requestContext);

    PropDataContext importFromDB(PropDataContext requestContext);

    PropDataContext addCommandAndWaitForComplete(PropDataContext requestContext);

    void createSingleTableFromAvro(PropDataContext requestContext) throws Exception;

    PropDataContext createSingleAVROFromTable(PropDataContext requestContext);

}
