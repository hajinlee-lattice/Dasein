package com.latticeengines.domain.exposed;

import java.util.List;

@SuppressWarnings("rawtypes")
public class SimpleBooleanResponse extends ResponseDocument {

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

}
