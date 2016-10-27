package com.latticeengines.domain.exposed.datacloud;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public final class EngineConstants {

    public static final CustomerSpace PRODATA_CUSTOMERSPACE = CustomerSpace
            .parse("PropDataService");
    public static final String SQOOP_CUSTOMER_PATTERN = "PropData~[%s]";
    public static final String INGESTION_DOWNLOADING = "._DOWNLOADING_";
    public static final String INGESTION_SUCCESS = "_SUCCESS";
    public static final String CSV = ".csv";
    public static final String CSV_GZ = ".csv.gz";
    public static final String OUT_GZ = ".OUT.gz";
    public static final String GZ = ".gz";
    public static final String AVRO = ".avro";

}
