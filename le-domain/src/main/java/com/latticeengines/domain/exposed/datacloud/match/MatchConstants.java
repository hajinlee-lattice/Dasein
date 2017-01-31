package com.latticeengines.domain.exposed.datacloud.match;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

public final class MatchConstants {
    public static final String CACHE_TABLE = "DerivedColumnsCache";
    public static final String IS_PUBLIC_DOMAIN = "IsPublicDomain";
    public static final String DISPOSABLE_EMAIL = "DisposableEmail";

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
    public static final String AM_COUNTRY_CODE_FIELD = "CURRENCY_CODE";

    static final String MODEL = Predefined.Model.getName();
    static final String DERIVED_COLUMNS = Predefined.DerivedColumns.getName();
    static final String RTS = Predefined.RTS.getName();

}
