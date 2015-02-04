package com.latticeengines.scoringharness;

import java.io.File;
import java.util.Arrays;
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
            return Arrays.asList(array);
        } catch (Exception e) {
            throw new RuntimeException("Error reading input file " + path, e);
        }
    }
}
