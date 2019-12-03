package com.latticeengines.dataflow.runtime.cascading.cdl;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class NumberOfContactsAggregator extends BaseAggregator<NumberOfContactsAggregator.Context>
        implements Aggregator<NumberOfContactsAggregator.Context> {

    private String contactIdField;

    public NumberOfContactsAggregator(String numberOfContactsField, String contactIdField) {
        super(new Fields(numberOfContactsField));
        this.contactIdField = contactIdField;
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        return false;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        return new Context();
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        String contactId = arguments.getString(contactIdField);
        // Skip counting rows where the Contact ID is null because that means
        // that the Account
        // had no Contacts after a Left Join.
        if (contactId != null) {
            context.numberOfContacts++;
        }
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        Tuple result = Tuple.size(1);
        result.set(0, context.numberOfContacts);
        return result;
    }

    public static class Context extends BaseAggregator.Context {
        Integer numberOfContacts = 0;
    }
}
