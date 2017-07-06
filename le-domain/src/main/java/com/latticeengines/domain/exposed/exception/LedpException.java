package com.latticeengines.domain.exposed.exception;

import org.apache.commons.lang3.exception.ExceptionUtils;

public class LedpException extends RuntimeException {

    private static final long serialVersionUID = 4758154339163073679L;
    private LedpCode code;

    public LedpException(LedpCode code) {
        this(code, null, new String[] {});
    }

    public LedpException(LedpCode code, Throwable t) {
        this(code, t, new String[] {});
    }

    public LedpException(LedpCode code, Object[] params) {
        this(code, null, params);
    }

    public LedpException(LedpCode code, Throwable t, Object[] params) {
        super(buildMessage(code, params), t);
        this.code = code;
    }

    public LedpCode getCode() {
        return code;
    }

    public LedpException(LedpCode code, String msg, Throwable t) {
        super(code.name() + ": " + msg, t);
        this.code = code;
    }

    public static String buildMessage(LedpCode code, Object[] params) {
        String msg = code.getMessage();

        for (int i = 0; i < params.length; i++) {
            Object param = params[i];
            if (param != null) {
                // we need to escape $ from param otherwise it interfere with
                // replace logic
                param = param.toString().replace("$", "\\$");
                msg = msg.replaceAll("\\{" + i + "\\}", param.toString());
            }
        }
        return msg;
    }

    public static String buildMessageWithCode(LedpCode code, String[] params) {
        String msg = buildMessage(code, params);
        return code.name() + ": " + msg;
    }

    public ErrorDetails getErrorDetails() {
        String stackTrace = ExceptionUtils.getStackTrace(this);
        return new ErrorDetails(code, getMessage(), stackTrace);
    }
}
