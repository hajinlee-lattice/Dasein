package com.latticeengines.dataflow.exposed.exception;



public class DataFlowException extends RuntimeException {

    private static final long serialVersionUID = -5429234001841295261L;

    private DataFlowCode code;

    public DataFlowException(DataFlowCode code) {
        this(code, null, new String[] {});
    }

    public DataFlowException(DataFlowCode code, Throwable t) {
        this(code, t, new String[] {});
    }

    public DataFlowException(DataFlowCode code, String[] params) {
        this(code, null, params);
    }

    public DataFlowException(DataFlowCode code, Throwable t, String[] params) {
        super(buildMessage(code, params), t);
        this.code = code;
    }
    
    public DataFlowCode getCode() {
        return code;
    }

    public DataFlowException(DataFlowCode code, String msg, Throwable t) {
        super(code.name() + ": " + msg, t);
    }

    public static String buildMessage(DataFlowCode code, String[] params) {
        String msg = code.getMessage();

        for (int i = 0; i < params.length; i++) {
            msg = msg.replaceAll("\\{" + i + "\\}", params[i]);
        }
        return msg;
    }
}
