package com.latticeengines.domain.exposed.scoringapi;

import java.util.EnumSet;

/**
 * Exposes different combination of FieldInterpretation collections, without
 * modifying the source enum.
 */
public class FieldInterpretationCollections {

    /**
     * Attribute collection to represent the Primary fields, that are used in
     * multiple places 1. Model fields needs to be marked as Primary for
     * required mappings 2. Scoring API accepts only these attributes for Fuzzy
     * matching 3. Account Lookup accepts only these attributes for Fuzzy
     * matching
     */
    public static final EnumSet<FieldInterpretation> PrimaryMatchingFields = (EnumSet<FieldInterpretation>) EnumSet.of(
            FieldInterpretation.Id, 
                    FieldInterpretation.CompanyName,
                    FieldInterpretation.Email, 
                    FieldInterpretation.Website, 
                    FieldInterpretation.City,
                    FieldInterpretation.State, 
                    FieldInterpretation.Country, 
                    FieldInterpretation.PostalCode,
                    FieldInterpretation.PhoneNumber);
}
