package com.latticeengines.dataplatform.exposed.exception;

public class LedpException extends RuntimeException {

    private static final long serialVersionUID = 4758154339163073679L;

    public LedpException(LedpCode code) {
        this(code, null, new String[] {});
    }

    public LedpException(LedpCode code, Throwable t) {
        this(code, t, new String[] {});
    }

    public LedpException(LedpCode code, String[] params) {
        this(code, null, params);
    }

    public LedpException(LedpCode code, Throwable t, String[] params) {
        super(buildMessage(code, params), t);

    }

    public LedpException(LedpCode code, String msg, Throwable t) {
        super(code.name() + ": " + msg, t);
    }

    public static String buildMessage(LedpCode code, String[] params) {
        String msg = code.getMessage();

        for (int i = 0; i < params.length; i++) {
            msg = msg.replaceAll("\\{" + i + "\\}", params[i]);
        }
        return msg;
    }

}
