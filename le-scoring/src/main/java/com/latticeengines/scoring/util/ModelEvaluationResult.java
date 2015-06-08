/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.latticeengines.scoring.util;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class ModelEvaluationResult extends org.apache.avro.specific.SpecificRecordBase implements
        org.apache.avro.specific.SpecificRecord {
    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser()
            .parse("{\"type\":\"record\",\"name\":\"ModelEvaluationResult\",\"namespace\":\"com.latticeengines.scoring.util\",\"fields\":[{\"name\":\"LeadID\",\"type\":[\"string\",\"null\"],\"columnName\":\"LeadID\",\"sqlType\":\"12\"},{\"name\":\"Bucket_Display_Name\",\"type\":[\"string\",\"null\"],\"columnName\":\"Bucket_Display_Name\",\"sqlType\":\"12\"},{\"name\":\"Lift\",\"type\":[\"double\",\"null\"],\"columnName\":\"Lift\",\"sqlType\":\"7\"},{\"name\":\"Play_Display_Name\",\"type\":[\"string\",\"null\"],\"columnName\":\"Play_Display_Name\",\"sqlType\":\"12\"},{\"name\":\"Percentile\",\"type\":[\"int\",\"null\"],\"columnName\":\"Percentile\",\"sqlType\":\"4\"},{\"name\":\"Probability\",\"type\":[\"double\",\"null\"],\"columnName\":\"Probability\",\"sqlType\":\"7\"},{\"name\":\"RawScore\",\"type\":[\"double\",\"null\"],\"columnName\":\"RawScore\",\"sqlType\":\"7\"},{\"name\":\"Score\",\"type\":[\"int\",\"null\"],\"columnName\":\"Score\",\"sqlType\":\"4\"}]}");

    public static org.apache.avro.Schema getClassSchema() {
        return SCHEMA$;
    }

    @Deprecated
    public java.lang.CharSequence LeadID;
    @Deprecated
    public java.lang.CharSequence Bucket_Display_Name;
    @Deprecated
    public java.lang.Double Lift;
    @Deprecated
    public java.lang.CharSequence Play_Display_Name;
    @Deprecated
    public java.lang.Integer Percentile;
    @Deprecated
    public java.lang.Double Probability;
    @Deprecated
    public java.lang.Double RawScore;
    @Deprecated
    public java.lang.Integer Score;

    /**
     * Default constructor. Note that this does not initialize fields to their
     * default values from the schema. If that is desired then one should use
     * <code>newBuilder()</code>.
     */
    public ModelEvaluationResult() {
    }

    /**
     * All-args constructor.
     */
    public ModelEvaluationResult(java.lang.CharSequence LeadID, java.lang.CharSequence Bucket_Display_Name,
            java.lang.Double Lift, java.lang.CharSequence Play_Display_Name, java.lang.Integer Percentile,
            java.lang.Double Probability, java.lang.Double RawScore, java.lang.Integer Score) {
        this.LeadID = LeadID;
        this.Bucket_Display_Name = Bucket_Display_Name;
        this.Lift = Lift;
        this.Play_Display_Name = Play_Display_Name;
        this.Percentile = Percentile;
        this.Probability = Probability;
        this.RawScore = RawScore;
        this.Score = Score;
    }

    public org.apache.avro.Schema getSchema() {
        return SCHEMA$;
    }

    // Used by DatumWriter. Applications should not call.
    public java.lang.Object get(int field$) {
        switch (field$) {
        case 0:
            return LeadID;
        case 1:
            return Bucket_Display_Name;
        case 2:
            return Lift;
        case 3:
            return Play_Display_Name;
        case 4:
            return Percentile;
        case 5:
            return Probability;
        case 6:
            return RawScore;
        case 7:
            return Score;
        default:
            throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    // Used by DatumReader. Applications should not call.
    @SuppressWarnings(value = "unchecked")
    public void put(int field$, java.lang.Object value$) {
        switch (field$) {
        case 0:
            LeadID = (java.lang.CharSequence) value$;
            break;
        case 1:
            Bucket_Display_Name = (java.lang.CharSequence) value$;
            break;
        case 2:
            Lift = (java.lang.Double) value$;
            break;
        case 3:
            Play_Display_Name = (java.lang.CharSequence) value$;
            break;
        case 4:
            Percentile = (java.lang.Integer) value$;
            break;
        case 5:
            Probability = (java.lang.Double) value$;
            break;
        case 6:
            RawScore = (java.lang.Double) value$;
            break;
        case 7:
            Score = (java.lang.Integer) value$;
            break;
        default:
            throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    /**
     * Gets the value of the 'LeadID' field.
     */
    public java.lang.CharSequence getLeadID() {
        return LeadID;
    }

    /**
     * Sets the value of the 'LeadID' field.
     * 
     * @param value
     *            the value to set.
     */
    public void setLeadID(java.lang.CharSequence value) {
        this.LeadID = value;
    }

    /**
     * Gets the value of the 'Bucket_Display_Name' field.
     */
    public java.lang.CharSequence getBucketDisplayName() {
        return Bucket_Display_Name;
    }

    /**
     * Sets the value of the 'Bucket_Display_Name' field.
     * 
     * @param value
     *            the value to set.
     */
    public void setBucketDisplayName(java.lang.CharSequence value) {
        this.Bucket_Display_Name = value;
    }

    /**
     * Gets the value of the 'Lift' field.
     */
    public java.lang.Double getLift() {
        return Lift;
    }

    /**
     * Sets the value of the 'Lift' field.
     * 
     * @param value
     *            the value to set.
     */
    public void setLift(java.lang.Double value) {
        this.Lift = value;
    }

    /**
     * Gets the value of the 'Play_Display_Name' field.
     */
    public java.lang.CharSequence getPlayDisplayName() {
        return Play_Display_Name;
    }

    /**
     * Sets the value of the 'Play_Display_Name' field.
     * 
     * @param value
     *            the value to set.
     */
    public void setPlayDisplayName(java.lang.CharSequence value) {
        this.Play_Display_Name = value;
    }

    /**
     * Gets the value of the 'Percentile' field.
     */
    public java.lang.Integer getPercentile() {
        return Percentile;
    }

    /**
     * Sets the value of the 'Percentile' field.
     * 
     * @param value
     *            the value to set.
     */
    public void setPercentile(java.lang.Integer value) {
        this.Percentile = value;
    }

    /**
     * Gets the value of the 'Probability' field.
     */
    public java.lang.Double getProbability() {
        return Probability;
    }

    /**
     * Sets the value of the 'Probability' field.
     * 
     * @param value
     *            the value to set.
     */
    public void setProbability(java.lang.Double value) {
        this.Probability = value;
    }

    /**
     * Gets the value of the 'RawScore' field.
     */
    public java.lang.Double getRawScore() {
        return RawScore;
    }

    /**
     * Sets the value of the 'RawScore' field.
     * 
     * @param value
     *            the value to set.
     */
    public void setRawScore(java.lang.Double value) {
        this.RawScore = value;
    }

    /**
     * Gets the value of the 'Score' field.
     */
    public java.lang.Integer getScore() {
        return Score;
    }

    /**
     * Sets the value of the 'Score' field.
     * 
     * @param value
     *            the value to set.
     */
    public void setScore(java.lang.Integer value) {
        this.Score = value;
    }

    /** Creates a new ModelEvaluationResult RecordBuilder */
    public static com.latticeengines.scoring.util.ModelEvaluationResult.Builder newBuilder() {
        return new com.latticeengines.scoring.util.ModelEvaluationResult.Builder();
    }

    /**
     * Creates a new ModelEvaluationResult RecordBuilder by copying an existing
     * Builder
     */
    public static com.latticeengines.scoring.util.ModelEvaluationResult.Builder newBuilder(
            com.latticeengines.scoring.util.ModelEvaluationResult.Builder other) {
        return new com.latticeengines.scoring.util.ModelEvaluationResult.Builder(other);
    }

    /**
     * Creates a new ModelEvaluationResult RecordBuilder by copying an existing
     * ModelEvaluationResult instance
     */
    public static com.latticeengines.scoring.util.ModelEvaluationResult.Builder newBuilder(
            com.latticeengines.scoring.util.ModelEvaluationResult other) {
        return new com.latticeengines.scoring.util.ModelEvaluationResult.Builder(other);
    }

    /**
     * RecordBuilder for ModelEvaluationResult instances.
     */
    public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<ModelEvaluationResult>
            implements org.apache.avro.data.RecordBuilder<ModelEvaluationResult> {

        private java.lang.CharSequence LeadID;
        private java.lang.CharSequence Bucket_Display_Name;
        private java.lang.Double Lift;
        private java.lang.CharSequence Play_Display_Name;
        private java.lang.Integer Percentile;
        private java.lang.Double Probability;
        private java.lang.Double RawScore;
        private java.lang.Integer Score;

        /** Creates a new Builder */
        private Builder() {
            super(com.latticeengines.scoring.util.ModelEvaluationResult.SCHEMA$);
        }

        /** Creates a Builder by copying an existing Builder */
        private Builder(com.latticeengines.scoring.util.ModelEvaluationResult.Builder other) {
            super(other);
            if (isValidValue(fields()[0], other.LeadID)) {
                this.LeadID = data().deepCopy(fields()[0].schema(), other.LeadID);
                fieldSetFlags()[0] = true;
            }
            if (isValidValue(fields()[1], other.Bucket_Display_Name)) {
                this.Bucket_Display_Name = data().deepCopy(fields()[1].schema(), other.Bucket_Display_Name);
                fieldSetFlags()[1] = true;
            }
            if (isValidValue(fields()[2], other.Lift)) {
                this.Lift = data().deepCopy(fields()[2].schema(), other.Lift);
                fieldSetFlags()[2] = true;
            }
            if (isValidValue(fields()[3], other.Play_Display_Name)) {
                this.Play_Display_Name = data().deepCopy(fields()[3].schema(), other.Play_Display_Name);
                fieldSetFlags()[3] = true;
            }
            if (isValidValue(fields()[4], other.Percentile)) {
                this.Percentile = data().deepCopy(fields()[4].schema(), other.Percentile);
                fieldSetFlags()[4] = true;
            }
            if (isValidValue(fields()[5], other.Probability)) {
                this.Probability = data().deepCopy(fields()[5].schema(), other.Probability);
                fieldSetFlags()[5] = true;
            }
            if (isValidValue(fields()[6], other.RawScore)) {
                this.RawScore = data().deepCopy(fields()[6].schema(), other.RawScore);
                fieldSetFlags()[6] = true;
            }
            if (isValidValue(fields()[7], other.Score)) {
                this.Score = data().deepCopy(fields()[7].schema(), other.Score);
                fieldSetFlags()[7] = true;
            }
        }

        /**
         * Creates a Builder by copying an existing ModelEvaluationResult
         * instance
         */
        private Builder(com.latticeengines.scoring.util.ModelEvaluationResult other) {
            super(com.latticeengines.scoring.util.ModelEvaluationResult.SCHEMA$);
            if (isValidValue(fields()[0], other.LeadID)) {
                this.LeadID = data().deepCopy(fields()[0].schema(), other.LeadID);
                fieldSetFlags()[0] = true;
            }
            if (isValidValue(fields()[1], other.Bucket_Display_Name)) {
                this.Bucket_Display_Name = data().deepCopy(fields()[1].schema(), other.Bucket_Display_Name);
                fieldSetFlags()[1] = true;
            }
            if (isValidValue(fields()[2], other.Lift)) {
                this.Lift = data().deepCopy(fields()[2].schema(), other.Lift);
                fieldSetFlags()[2] = true;
            }
            if (isValidValue(fields()[3], other.Play_Display_Name)) {
                this.Play_Display_Name = data().deepCopy(fields()[3].schema(), other.Play_Display_Name);
                fieldSetFlags()[3] = true;
            }
            if (isValidValue(fields()[4], other.Percentile)) {
                this.Percentile = data().deepCopy(fields()[4].schema(), other.Percentile);
                fieldSetFlags()[4] = true;
            }
            if (isValidValue(fields()[5], other.Probability)) {
                this.Probability = data().deepCopy(fields()[5].schema(), other.Probability);
                fieldSetFlags()[5] = true;
            }
            if (isValidValue(fields()[6], other.RawScore)) {
                this.RawScore = data().deepCopy(fields()[6].schema(), other.RawScore);
                fieldSetFlags()[6] = true;
            }
            if (isValidValue(fields()[7], other.Score)) {
                this.Score = data().deepCopy(fields()[7].schema(), other.Score);
                fieldSetFlags()[7] = true;
            }
        }

        /** Gets the value of the 'LeadID' field */
        public java.lang.CharSequence getLeadID() {
            return LeadID;
        }

        /** Sets the value of the 'LeadID' field */
        public com.latticeengines.scoring.util.ModelEvaluationResult.Builder setLeadID(java.lang.CharSequence value) {
            validate(fields()[0], value);
            this.LeadID = value;
            fieldSetFlags()[0] = true;
            return this;
        }

        /** Checks whether the 'LeadID' field has been set */
        public boolean hasLeadID() {
            return fieldSetFlags()[0];
        }

        /** Clears the value of the 'LeadID' field */
        public com.latticeengines.scoring.util.ModelEvaluationResult.Builder clearLeadID() {
            LeadID = null;
            fieldSetFlags()[0] = false;
            return this;
        }

        /** Gets the value of the 'Bucket_Display_Name' field */
        public java.lang.CharSequence getBucketDisplayName() {
            return Bucket_Display_Name;
        }

        /** Sets the value of the 'Bucket_Display_Name' field */
        public com.latticeengines.scoring.util.ModelEvaluationResult.Builder setBucketDisplayName(
                java.lang.CharSequence value) {
            validate(fields()[1], value);
            this.Bucket_Display_Name = value;
            fieldSetFlags()[1] = true;
            return this;
        }

        /** Checks whether the 'Bucket_Display_Name' field has been set */
        public boolean hasBucketDisplayName() {
            return fieldSetFlags()[1];
        }

        /** Clears the value of the 'Bucket_Display_Name' field */
        public com.latticeengines.scoring.util.ModelEvaluationResult.Builder clearBucketDisplayName() {
            Bucket_Display_Name = null;
            fieldSetFlags()[1] = false;
            return this;
        }

        /** Gets the value of the 'Lift' field */
        public java.lang.Double getLift() {
            return Lift;
        }

        /** Sets the value of the 'Lift' field */
        public com.latticeengines.scoring.util.ModelEvaluationResult.Builder setLift(java.lang.Double value) {
            validate(fields()[2], value);
            this.Lift = value;
            fieldSetFlags()[2] = true;
            return this;
        }

        /** Checks whether the 'Lift' field has been set */
        public boolean hasLift() {
            return fieldSetFlags()[2];
        }

        /** Clears the value of the 'Lift' field */
        public com.latticeengines.scoring.util.ModelEvaluationResult.Builder clearLift() {
            Lift = null;
            fieldSetFlags()[2] = false;
            return this;
        }

        /** Gets the value of the 'Play_Display_Name' field */
        public java.lang.CharSequence getPlayDisplayName() {
            return Play_Display_Name;
        }

        /** Sets the value of the 'Play_Display_Name' field */
        public com.latticeengines.scoring.util.ModelEvaluationResult.Builder setPlayDisplayName(
                java.lang.CharSequence value) {
            validate(fields()[3], value);
            this.Play_Display_Name = value;
            fieldSetFlags()[3] = true;
            return this;
        }

        /** Checks whether the 'Play_Display_Name' field has been set */
        public boolean hasPlayDisplayName() {
            return fieldSetFlags()[3];
        }

        /** Clears the value of the 'Play_Display_Name' field */
        public com.latticeengines.scoring.util.ModelEvaluationResult.Builder clearPlayDisplayName() {
            Play_Display_Name = null;
            fieldSetFlags()[3] = false;
            return this;
        }

        /** Gets the value of the 'Percentile' field */
        public java.lang.Integer getPercentile() {
            return Percentile;
        }

        /** Sets the value of the 'Percentile' field */
        public com.latticeengines.scoring.util.ModelEvaluationResult.Builder setPercentile(java.lang.Integer value) {
            validate(fields()[4], value);
            this.Percentile = value;
            fieldSetFlags()[4] = true;
            return this;
        }

        /** Checks whether the 'Percentile' field has been set */
        public boolean hasPercentile() {
            return fieldSetFlags()[4];
        }

        /** Clears the value of the 'Percentile' field */
        public com.latticeengines.scoring.util.ModelEvaluationResult.Builder clearPercentile() {
            Percentile = null;
            fieldSetFlags()[4] = false;
            return this;
        }

        /** Gets the value of the 'Probability' field */
        public java.lang.Double getProbability() {
            return Probability;
        }

        /** Sets the value of the 'Probability' field */
        public com.latticeengines.scoring.util.ModelEvaluationResult.Builder setProbability(java.lang.Double value) {
            validate(fields()[5], value);
            this.Probability = value;
            fieldSetFlags()[5] = true;
            return this;
        }

        /** Checks whether the 'Probability' field has been set */
        public boolean hasProbability() {
            return fieldSetFlags()[5];
        }

        /** Clears the value of the 'Probability' field */
        public com.latticeengines.scoring.util.ModelEvaluationResult.Builder clearProbability() {
            Probability = null;
            fieldSetFlags()[5] = false;
            return this;
        }

        /** Gets the value of the 'RawScore' field */
        public java.lang.Double getRawScore() {
            return RawScore;
        }

        /** Sets the value of the 'RawScore' field */
        public com.latticeengines.scoring.util.ModelEvaluationResult.Builder setRawScore(java.lang.Double value) {
            validate(fields()[6], value);
            this.RawScore = value;
            fieldSetFlags()[6] = true;
            return this;
        }

        /** Checks whether the 'RawScore' field has been set */
        public boolean hasRawScore() {
            return fieldSetFlags()[6];
        }

        /** Clears the value of the 'RawScore' field */
        public com.latticeengines.scoring.util.ModelEvaluationResult.Builder clearRawScore() {
            RawScore = null;
            fieldSetFlags()[6] = false;
            return this;
        }

        /** Gets the value of the 'Score' field */
        public java.lang.Integer getScore() {
            return Score;
        }

        /** Sets the value of the 'Score' field */
        public com.latticeengines.scoring.util.ModelEvaluationResult.Builder setScore(java.lang.Integer value) {
            validate(fields()[7], value);
            this.Score = value;
            fieldSetFlags()[7] = true;
            return this;
        }

        /** Checks whether the 'Score' field has been set */
        public boolean hasScore() {
            return fieldSetFlags()[7];
        }

        /** Clears the value of the 'Score' field */
        public com.latticeengines.scoring.util.ModelEvaluationResult.Builder clearScore() {
            Score = null;
            fieldSetFlags()[7] = false;
            return this;
        }

        @Override
        public ModelEvaluationResult build() {
            try {
                ModelEvaluationResult record = new ModelEvaluationResult();
                record.LeadID = fieldSetFlags()[0] ? this.LeadID : (java.lang.CharSequence) defaultValue(fields()[0]);
                record.Bucket_Display_Name = fieldSetFlags()[1] ? this.Bucket_Display_Name
                        : (java.lang.CharSequence) defaultValue(fields()[1]);
                record.Lift = fieldSetFlags()[2] ? this.Lift : (java.lang.Double) defaultValue(fields()[2]);
                record.Play_Display_Name = fieldSetFlags()[3] ? this.Play_Display_Name
                        : (java.lang.CharSequence) defaultValue(fields()[3]);
                record.Percentile = fieldSetFlags()[4] ? this.Percentile
                        : (java.lang.Integer) defaultValue(fields()[4]);
                record.Probability = fieldSetFlags()[5] ? this.Probability
                        : (java.lang.Double) defaultValue(fields()[5]);
                record.RawScore = fieldSetFlags()[6] ? this.RawScore : (java.lang.Double) defaultValue(fields()[6]);
                record.Score = fieldSetFlags()[7] ? this.Score : (java.lang.Integer) defaultValue(fields()[7]);
                return record;
            } catch (Exception e) {
                throw new org.apache.avro.AvroRuntimeException(e);
            }
        }
    }
}
