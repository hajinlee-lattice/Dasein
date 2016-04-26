package com.latticeengines.serviceruntime.exposed.exception;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.ResponseErrorHandler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.RemoteLedpException;

public class GetResponseErrorHandler implements ResponseErrorHandler {
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(GetResponseErrorHandler.class);

    public GetResponseErrorHandler() {
    }

    @Override
    public boolean hasError(ClientHttpResponse response) throws IOException {
        return response.getStatusCode() != HttpStatus.OK;
    }

    @Override
    public void handleError(ClientHttpResponse response) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copy(response.getBody(), baos);
        String body = new String(baos.toByteArray());
        if (!interpretAndThrowException(response.getStatusCode(), body)) {
            throw new RuntimeException(body);
        }
    }

    private boolean interpretAndThrowException(HttpStatus status, String body) {
        RemoteLedpException exception;
        try {
            JsonNode node = new ObjectMapper().readTree(body);
            JsonNode stackTrace = node.get("stackTrace");
            String stackTraceString = null;
            if (stackTrace != null) {
                stackTraceString = stackTrace.asText();
            }

            LedpCode code = LedpCode.valueOf(node.get("errorCode").asText());
            String message = node.get("errorMsg").asText();
            Throwable remoteException = generateRemoteException(stackTraceString);
            exception = new RemoteLedpException(status, code, message, remoteException);
        } catch (Exception e) {
            return false;
        }
        throw exception;
    }

    public Exception generateRemoteException(String trace) {
        Pattern headLinePattern = Pattern.compile("([\\w\\.]+)(:.*)?");
        Matcher headLineMatcher = headLinePattern.matcher(trace);
        if (headLineMatcher.find()) {
            System.out.println("Headline: " + headLineMatcher.group(1));
            if (headLineMatcher.group(2) != null) {
                System.out.println("Optional message " + headLineMatcher.group(2));
            }
        }
        // "at package.class.method(source.java:123)"
        Pattern tracePattern = Pattern
                .compile("\\s*at\\s+([\\w\\.$_]+)\\.([\\w$_]+)(\\(.*java)?:(\\d+)\\)(\\n|\\r\\n)");
        Matcher traceMatcher = tracePattern.matcher(trace);
        List<StackTraceElement> stackTrace = new ArrayList<StackTraceElement>();
        while (traceMatcher.find()) {
            String className = traceMatcher.group(1);
            String methodName = traceMatcher.group(2);
            String sourceFile = traceMatcher.group(3);
            int lineNum = Integer.parseInt(traceMatcher.group(4));
            stackTrace.add(new StackTraceElement(className, methodName, sourceFile, lineNum));
        }

        Exception exception = new Exception();
        exception.setStackTrace(stackTrace.toArray(new StackTraceElement[] {}));
        return exception;
    }
}
