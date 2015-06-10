package com.latticeengines.scoring.util;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;

public class DatatypeValidationResult {

    private ArrayList<String> datatypeFailures;

    private Hashtable<String, ArrayList<String>> modelFailures;

    DatatypeValidationResult(ArrayList<String> df, Hashtable<String, ArrayList<String>> mf) {
        datatypeFailures = df;
        modelFailures = mf;
    }

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
                ArrayList<String> msgs = modelFailures.get(model);
                for (String msg : msgs)
                    sb.append(msg);
                sb.append("\t");
            }
        }
        return sb.toString();
    }

    public ArrayList<String> getDatatypeFailures() {
        return datatypeFailures;
    }

    public Hashtable<String, ArrayList<String>> getModelFailures() {
        return modelFailures;
    }

    public boolean passDatatypeValidation() {
        ArrayList<String> df = getDatatypeFailures();
        Hashtable<String, ArrayList<String>> mf = getModelFailures();
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
