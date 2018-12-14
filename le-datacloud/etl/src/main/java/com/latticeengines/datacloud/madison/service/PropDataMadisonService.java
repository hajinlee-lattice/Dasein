package com.latticeengines.datacloud.madison.service;

public interface PropDataMadisonService {

    String TODAY_KEY = "today";
    String RECORD_KEY = "record";
    String RESULT_KEY = "result";
    String STATUS_KEY = "status";
    String STATUS_OK = "OK";

    PropDataContext importFromDB(PropDataContext requestContext);

    PropDataContext transform(PropDataContext requestContext);

    PropDataContext exportToDB(PropDataContext requestContext);

}
