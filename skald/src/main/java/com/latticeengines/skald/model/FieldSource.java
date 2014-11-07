package com.latticeengines.skald.model;

public enum FieldSource {
    // Data for this field will be provided along with the scoring request.
    Request,

    // Data for this field is available through the internal Prop Data system.
    Internal,

    //
    Customer
}
