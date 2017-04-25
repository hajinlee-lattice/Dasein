package com.latticeengines.datacloud.core.source;

/*
 * List of Ingestion names for source firehose. For these firehose model,
 * data is collected from various places (like SFTP, URL, DB etc)
 * and is downloaded under specific versioned HDFS directory
 *
 * IMPORTANT NOTE: New constants could be added but values should not be
 * changed once created
 */
public class IngestionNames {
    public static final String BOMBORA_FIREHOSE = "BomboraFirehose";
    public static final String CACHESEED_CSVGZ = "CacheSeedCsvGz";
    public static final String DNB_CASHESEED = "DnBCacheSeed";
    public static final String ORB_INTELLIGENCE = "OrbIntelligence";
    public static final String HGDATA = "HGData";
    public static final String BOMBORA_SURGE = "BomboraSurge";
}
