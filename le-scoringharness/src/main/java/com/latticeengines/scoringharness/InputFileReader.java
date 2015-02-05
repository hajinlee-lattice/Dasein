package com.latticeengines.scoringharness;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.scoringharness.operationmodel.OperationSpec;

public class InputFileReader {
    private String path;

    public InputFileReader(String path) {
        this.path = path;
    }

    public List<OperationSpec> read() {
        try {
            OperationSpec[] array = new ObjectMapper().readValue(new File(path), OperationSpec[].class);

            // Sort OperationSpecs
            Arrays.sort(array, new Comparator<OperationSpec>() {
                @Override
                public int compare(OperationSpec spec1, OperationSpec spec2) {
                    if (spec1.offsetMilliseconds < spec2.offsetMilliseconds)
                        return -1;
                    if (spec1.offsetMilliseconds == spec2.offsetMilliseconds)
                        return 0;
                    return 1;
                }
            });
            return Arrays.asList(array);
        } catch (Exception e) {
            throw new RuntimeException("Error reading input file " + path, e);
        }
    }
}
