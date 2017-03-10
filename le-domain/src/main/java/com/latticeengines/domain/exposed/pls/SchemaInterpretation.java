package com.latticeengines.domain.exposed.pls;

import com.latticeengines.domain.exposed.metadata.InterfaceName;

public enum SchemaInterpretation {
    SalesforceAccount, //
    SalesforceLead, //
    TestingData, //
    Account, //
    Contact, //
    TimeSeries, //
    Category, //
    BucketedAccountMaster;

    public void apply(SchemaInterpretationFunctionalInterface function) {
        InterfaceName name = null;
        switch (this) {
        case SalesforceAccount:
            name = InterfaceName.Website;
            break;
        case SalesforceLead:
            name = InterfaceName.Email;
            break;
        default:
            break;
        }
        function.actOnSchemaInterpretation(name);
    }
}
