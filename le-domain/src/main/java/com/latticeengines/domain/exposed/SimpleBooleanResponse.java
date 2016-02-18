package com.latticeengines.domain.exposed;

import java.util.Arrays;
import java.util.List;

@SuppressWarnings("rawtypes")
public class SimpleBooleanResponse extends ResponseDocument {

    private SimpleBooleanResponse() {
    }

    @SuppressWarnings("unchecked")
    private SimpleBooleanResponse(boolean success, List<String> errors) {
        this.setSuccess(success);
        this.setErrors(errors);
    }

    public static SimpleBooleanResponse successResponse() {
        return new SimpleBooleanResponse(true, null);
    }

    public static SimpleBooleanResponse failedResponse(List<String> errors) {
        return new SimpleBooleanResponse(false, errors);
    }

    public static SimpleBooleanResponse failedResponse(Exception e) {
        return new SimpleBooleanResponse(false, Arrays.asList(e.getMessage()));
    }
}
