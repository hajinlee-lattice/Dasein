package com.latticeengines.domain.exposed.skald.model;

public enum FieldSource {
    // Provided along with the scoring request.
    REQUEST,

    // Available through the internal Prop Data system.
    PROPRIETARY,

    // Extracted from customer systems through a secondary process.
    CUSTOMER
}
