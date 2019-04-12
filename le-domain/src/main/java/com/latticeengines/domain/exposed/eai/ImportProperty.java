package com.latticeengines.domain.exposed.eai;


import com.latticeengines.domain.exposed.BaseProperty;

public class ImportProperty extends BaseProperty {

    public static final String TABLE = "table";
    public static final String EAICONFIG = "eaiConfig";
    public static final String IMPORTCTX = "importContext";
    public static final String METADATAFILE = "metadataFile";
    public static final String METADATA = "metadata";
    public static final String PRODUCERTEMPLATE = "producerTemplate";
    public static final String FILEURLPROPERTIES = "fileUrlProperties";
    public static final String EXTRACT_PATH = "extractPaths";
    public static final String LAST_MODIFIED_DATE = "lastModifiedDate";
    public static final String PROCESSED_RECORDS = "processedRecords";
    public static final String METADATAURL = "metadataUrl";
    public static final String IMPORT_CONFIG_STR = "importConfigStr";
    public static final String COLLECTION_IDENTIFIERS = "collectionIdentifiers";
    public static final String EAIJOBDETAILIDS = "eaiJobDetailIds";
    public static final String MULTIPLE_EXTRACT = "multipleExtract";
    public static final String EXTRACT_PATH_LIST = "extractPathList";
    public static final String EXTRACT_RECORDS_LIST = "extractRecordsList";
    public static final String DEDUP_ENABLE = "dedupEnable";
    public static final String ID_COLUMN_NAME = "idColumnName";
    public static final String IGNORED_ROWS_LIST = "ignoredRowsList";
    public static final String IGNORED_ROWS = "ignoredRows";
    public static final String DUPLICATE_ROWS_LIST = "duplicateRowsList";
    public static final String DUPLICATE_ROWS = "duplciateRows";
    public static final String SKIP_UPDATE_ATTR_NAME = "skipUpdateAttrName";
    public static final String BUSINESS_ENTITY = "businessEntity";
    public static final String ERROR_FILE = "error.csv";
    public static final String[] ERROR_HEADER = new String[] { "LineNumber", "Id", "ErrorMessage" };
}
