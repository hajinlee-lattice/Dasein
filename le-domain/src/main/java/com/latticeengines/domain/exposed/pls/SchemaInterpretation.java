package com.latticeengines.domain.exposed.pls;

import com.latticeengines.domain.exposed.metadata.InterfaceName;

public enum SchemaInterpretation {
    SalesforceAccount, //
    SalesforceLead, //
    TestingData, //
    Account, //
    Contact, //
    Product, //
    Transaction, //
    TimeSeries, //
    Category, //
    Profile, //
    AccountMaster;

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
