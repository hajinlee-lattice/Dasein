package com.latticeengines.domain.exposed.datacloud.match;

import java.util.Arrays;
import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

public final class MatchConstants {
    public static final String CACHE_TABLE = "DerivedColumnsCache";
    public static final String IS_PUBLIC_DOMAIN = "IsPublicDomain";
    public static final String DISPOSABLE_EMAIL = "DisposableEmail";
    public static final String PREMATCH_DOMAIN = "PreMatchDomain";

    public static final String SOURCE_PREFIX = "Source_";

    // these are the same as column names in RTS Cache, if applicable
    public static final String DOMAIN_FIELD = "Domain";
    public static final String NAME_FIELD = "Name";
    public static final String COUNTRY_FIELD = "Country";
    public static final String STATE_FIELD = "State";
    public static final String CITY_FIELD = "City";
    public static final String ZIPCODE_FIELD = "ZipCode";
    public static final String PHONE_NUM_FIELD = "PhoneNumber";
    public static final String EMAIL_FIELD = "Email";
    public static final String DUNS_FIELD = "DUNS";
    public static final String COUNTRY_CODE_FIELD = "CountryCode";
    public static final String LID_FIELD = "LatticeAccountId";

    // these are the same as column names in AM, if applicable
    public static final String AM_DOMAIN_FIELD = "Domain";
    public static final String AM_NAME_FIELD = "LDC_Name";
    public static final String AM_COUNTRY_FIELD = "LDC_Country";
    public static final String AM_STATE_FIELD = "LDC_State";
    public static final String AM_CITY_FIELD = "LDC_City";
    public static final String AM_ZIPCODE_FIELD = "LDC_ZipCode";
    public static final String AM_PHONE_NUM_FIELD = "LE_COMPANY_PHONE";
    public static final String AM_DUNS_FIELD = "LDC_DUNS";
    public static final String AM_DDUNS_FIELD = "LE_PRIMARY_DUNS";
    public static final String AM_COUNTRY_CODE_FIELD = "CURRENCY_CODE";
    public static final String AM_EMPLOYEE_RANGE_FIELD = "LE_EMPLOYEE_RANGE";
    public static final String AM_REVENUE_RANGE_FIELD = "LE_REVENUE_RANGE";
    public static final String AM_PRIMARY_INDUSTRY_FIELD = "LDC_PrimaryIndustry";
    public static final String AM_SECOND_INDUSTRY_FIELD = "LE_INDUSTRY";
    public static final String AM_DOMAIN_SOURCE = "LDC_DomainSource";

    // those are the internal avro attributes
    public static final String INT_LDC_LID = "__LDC_LID__";
    public static final String INT_LDC_DEDUPE_ID = "__LDC_DedupeId__";
    public static final String INT_LDC_REMOVED = "__LDC_Removed__";

    public static final String TMP_BEST_DEDUPE_ID = "__Best_DedupeId__";

    public static final String INT_MATCHED_DUNS = "__Matched_DUNS__";
    public static final String INT_MATCHED_ORIG_DUNS = "__Matched_Orig_DUNS__";
    public static final String INT_MATCHED_CONFIDENCE_CODE = "__Matched_Confidence_Code__";
    public static final String INT_MATCHED_MATCH_GRADE = "__Matched_Match_Grade__";
    public static final String INT_MATCHED_CACHE_HIT = "__Matched_Cache_Hit__";
    public static final String INT_MATCHED_PASS_AC = "__Matched_Pass_AC__";
    public static final String INT_MATCHED_NAME = "__Matched_Name__";
    public static final String INT_MATCHED_ADDRESS = "__Matched_Address__";
    public static final String INT_MATCHED_CITY = "__Matched_City__";
    public static final String INT_MATCHED_STATE = "__Matched_State__";
    public static final String INT_MATCHED_COUNTRY_CODE = "__Matched_Country_Code__";
    public static final String INT_MATCHED_ZIPCODE = "__Matched_Zipcode__";
    public static final String INT_MATCHED_PHONE = "__Matched_Phone__";

    public static final List<String> matchDebugFields = Arrays.asList(INT_MATCHED_DUNS,
            INT_MATCHED_ORIG_DUNS, INT_MATCHED_CONFIDENCE_CODE, INT_MATCHED_MATCH_GRADE,
            INT_MATCHED_CACHE_HIT, INT_MATCHED_PASS_AC, INT_MATCHED_NAME, INT_MATCHED_ADDRESS,
            INT_MATCHED_CITY, INT_MATCHED_STATE, INT_MATCHED_COUNTRY_CODE, INT_MATCHED_ZIPCODE,
            INT_MATCHED_PHONE);
    // For MatchMonitorService
    public static final String REQUEST_NUM = "RequestNum";
    public static final String BATCH_NUM = "BatchNum";
    public static final String ADDRESS_NUM = "AddressNum";
    public static final String ACTIVE_REQ_NUM = "ActiveRequestNum";
    public static final String QUEUED_REQ_NUM = "QueuedRequestNum";
    static final String MODEL = Predefined.Model.getName();
    static final String DERIVED_COLUMNS = Predefined.DerivedColumns.getName();
    static final String RTS = Predefined.RTS.getName();

    public static final long TERMINATE_EXECUTOR_TIMEOUT_MS = 30_000;
}
