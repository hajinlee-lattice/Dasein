package com.latticeengines.domain.exposed.pls;

import java.util.List;

public class SimpleBooleanResponse extends ResponseDocument {

    private SimpleBooleanResponse(boolean success, List<String> errors) {
        this.setSuccess(success);
        this.setErrors(errors);
    }

    public static SimpleBooleanResponse getSuccessResponse() {
        return new SimpleBooleanResponse(true, null);
    }

    public static SimpleBooleanResponse getFailResponse(List<String> errors) {
        return new SimpleBooleanResponse(false, errors);
    }

}
