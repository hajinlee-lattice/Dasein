package com.latticeengines.skald.model;

public enum FieldSource {
    // Provided along with the scoring request.
    Request,

    // Available through the internal Prop Data system.
    Internal,

    // Extracted from customer systems through a secondary process.
    Customer
}
