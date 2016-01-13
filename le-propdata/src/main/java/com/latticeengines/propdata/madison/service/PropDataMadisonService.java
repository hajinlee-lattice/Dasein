package com.latticeengines.propdata.madison.service;

public interface PropDataMadisonService {

    public static final String TODAY_KEY = "today";
    public static final String RECORD_KEY = "record";
    public static final String RESULT_KEY = "result";
    public static final String STATUS_KEY = "status";
    public static final String STATUS_OK = "OK";

    PropDataContext importFromDB(PropDataContext requestContext);

    PropDataContext transform(PropDataContext requestContext);

    PropDataContext exportToDB(PropDataContext requestContext);

}
