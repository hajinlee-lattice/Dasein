package com.latticeengines.scoringapi.match;

import java.util.List;

// Note that this does not follow Java naming conventions because
// it's a result structure from another service.
public class ProprietaryDataResult {
    public List<ErrorStructure> Errors;
    public boolean Success;
    public ResultStructure Result;

    public static class ResultStructure {
        public List<ValueStructure> Values;
    }

    public static class ValueStructure {
        public StringStructure Key;
        public StringStructure Value;
    }

    public static class StringStructure {
        public String Value;
    }

    public static class ErrorStructure {
        public String Message;
    }
}
