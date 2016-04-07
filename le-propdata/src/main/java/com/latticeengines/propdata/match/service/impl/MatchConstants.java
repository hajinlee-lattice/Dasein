package com.latticeengines.propdata.match.service.impl;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

public final class MatchConstants {
    public static final String CACHE_TABLE = "DerivedColumnsCache";
    public static final String IS_PUBLIC_DOMAIN = "IsPublicDomain";
    public static final String DISPOSABLE_EMAIL = "DisposableEmail";
    public static final String MODEL = ColumnSelection.Predefined.Model.getName();
    public static final String DERIVED_COLUMNS = ColumnSelection.Predefined.DerivedColumns.getName();
    public static final String SERVICE_CUSTOMERSPACE = CustomerSpace.parse("PropDataService").toString();
    public static final String OUTPUT_RECORD_PREFIX = "PropDataOutput";
    public static final String SOURCE_FIELD_PREFIX = "Source_";
    public static final String PDSERVICE_TENANT = "PDService";
}
