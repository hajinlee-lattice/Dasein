package com.latticeengines.domain.exposed.pls;

import com.latticeengines.domain.exposed.metadata.InterfaceName;

public enum SchemaInterpretation {
    SalesforceAccount, //
    SalesforceLead, //
    TestingData, //
    Account, //
    ModelAccount, //
    Contact, //
    Product, //
    Transaction, //
    TimeSeries, //
    Category, //
    Profile, //
    AccountMaster, //
    TransactionRaw, //
    TransactionDailyAggregation, //
    TransactionPeriodAggregation, DeleteAccountTemplate, DeleteContactTemplate, DeleteTransactionTemplate,
    ContactEntityMatch;

    public static SchemaInterpretation getByName(String interpretationName) {
        for (SchemaInterpretation interpretation : values()) {
            if (interpretation.name().equalsIgnoreCase(interpretationName)) {
                return interpretation;
            }
        }
        throw new IllegalArgumentException(String.format(
                "There is no interpretation name %s in SchemaInterpretation", interpretationName));
    }

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
