package com.latticeengines.domain.exposed.exception;

public class LedpException extends RuntimeException {

    private static final long serialVersionUID = 4758154339163073679L;
    private LedpCode code;

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
        this.code = code;
    }
    
    public LedpCode getCode() {
        return code;
    }

    public LedpException(LedpCode code, String msg, Throwable t) {
        super(code.name() + ": " + msg, t);
        this.code = code;
    }

    public static String buildMessage(LedpCode code, String[] params) {
        String msg = code.getMessage();

        for (int i = 0; i < params.length; i++) {
            msg = msg.replaceAll("\\{" + i + "\\}", params[i]);
        }
        return msg;
    }

    public static String buildMessageWithCode(LedpCode code, String[] params) {
        String msg = buildMessage(code, params);
        return code.name() + ": " + msg;
    }
}
