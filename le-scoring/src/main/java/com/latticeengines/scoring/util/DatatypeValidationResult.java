package com.latticeengines.scoring.util;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DatatypeValidationResult {

    private List<String> datatypeFailures;

    private Map<String, List<String>> modelFailures;

    DatatypeValidationResult(List<String> df, Map<String, List<String>> mf) {
        datatypeFailures = df;
        modelFailures = mf;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("datatypeFailures:\n");
        if (datatypeFailures.size() == 0) {
            sb.append("none");
        } else {
            for (String dfStr : datatypeFailures) {
                sb.append(dfStr);
                sb.append("\t");
            }
        }
        sb.append("\nmodelFailures:\n");
        if (modelFailures.size() == 0) {
            sb.append("none");
        } else {
            Set<String> keys = modelFailures.keySet();
            for (String model : keys) {
                sb.append(model);
                sb.append(": ");
                List<String> msgs = modelFailures.get(model);
                for (String msg : msgs)
                    sb.append(msg);
                sb.append("\t");
            }
        }
        return sb.toString();
    }

    public List<String> getDatatypeFailures() {
        return datatypeFailures;
    }

    public Map<String, List<String>> getModelFailures() {
        return modelFailures;
    }

    public boolean passDatatypeValidation() {
        List<String> df = getDatatypeFailures();
        Map<String, List<String>> mf = getModelFailures();
        if (df == null && mf == null) {
            return true;
        } else if (df == null && mf != null) {
            return mf.size() == 0;
        } else if (df != null && mf == null) {
            return df.size() == 0;
        } else {
            return df.size() == 0 && mf.size() == 0;
        }
    }
}
