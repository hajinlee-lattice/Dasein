package com.latticeengines.domain.exposed.metadata;

import java.util.List;

import com.latticeengines.domain.exposed.db.PropertyBag;

public class DataCollectionPropertyBag extends PropertyBag<DataCollectionProperty, DataCollectionPropertyName> {
    public DataCollectionPropertyBag(List<DataCollectionProperty> bag) {
        super(bag);
    }
}
