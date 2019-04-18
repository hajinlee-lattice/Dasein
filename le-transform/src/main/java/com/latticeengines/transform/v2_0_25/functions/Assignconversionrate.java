package com.latticeengines.transform.v2_0_25.functions;

public class Assignconversionrate extends ColumnToValueToDoubleLookup {

    private static final long serialVersionUID = -7511040173923099149L;

    public Assignconversionrate() {
    }

    public Assignconversionrate(String modelPath) {
        importLookupMapFromJson(modelPath + "/conversionratemapping.json");
    }

}
