package com.latticeengines.common.exposed.rest;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ReadListener;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// Filter that overloads servlet requests and responses to allow the raw body to be read multiple times.
public class BodyBufferFilter implements Filter {
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // Pass
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException,
            ServletException {
        BufferedServletRequest bufferedRequest = new BufferedServletRequest((HttpServletRequest) request);
        BufferedServletResponse bufferedResponse = new BufferedServletResponse((HttpServletResponse) response);
        chain.doFilter(bufferedRequest, bufferedResponse);
    }

    @Override
    public void destroy() {
        // Pass
    }

    public static class BufferedServletInputStream extends ServletInputStream {
        public BufferedServletInputStream(byte[] body) {
            inner = new ByteArrayInputStream(body);
        }

        @Override
        public boolean isFinished() {
            return inner.available() == 0;
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setReadListener(ReadListener readListener) {
            // TODO Research if ignoring this causes any problems.

            // Pass
        }

        @Override
        public int read() throws IOException {
            return inner.read();
        }

        private final ByteArrayInputStream inner;
    }

    public static class BufferedServletRequest extends HttpServletRequestWrapper {
        public BufferedServletRequest(HttpServletRequest request) {
            super(request);

            try {
                body = IOUtils.toByteArray(request.getInputStream());
            } catch (IOException ex) {
                throw new RuntimeException("Failed to read request body", ex);
            }
        }

        @Override
        public ServletInputStream getInputStream() throws IOException {
            log.error("getInputStream");
            return new BufferedServletInputStream(body);
        }

        @Override
        public BufferedReader getReader() throws IOException {
            log.error("getReader");
            return new BufferedReader(new InputStreamReader(getInputStream(), "UTF-8"));
        }

        public byte[] getBody() {
            return body;
        }

        private byte[] body;

        private static final Log log = LogFactory.getLog(BufferedServletRequest.class);
    }

    public static class BufferedServletOutputStream extends ServletOutputStream {
        public BufferedServletOutputStream(ServletOutputStream output) {
            inner = output;
            buffer = new ByteArrayOutputStream();
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setWriteListener(WriteListener writeListener) {
            // TODO Research if ignoring this causes any problems.

            // Pass
        }

        @Override
        public void write(int b) throws IOException {
            inner.write(b);
            buffer.write(b);
        }

        public byte[] getBytes() {
            return buffer.toByteArray();
        }

        private final ServletOutputStream inner;
        private final ByteArrayOutputStream buffer;
    }

    public static class BufferedServletResponse extends HttpServletResponseWrapper {
        public BufferedServletResponse(HttpServletResponse response) {
            super(response);

            try {
                output = new BufferedServletOutputStream(response.getOutputStream());
            } catch (IOException ex) {
                throw new RuntimeException("Failed to get output stream", ex);
            }
        }

        @Override
        public ServletOutputStream getOutputStream() throws IOException {
            return output;
        }

        public byte[] getBody() {
            return output.getBytes();
        }

        private BufferedServletOutputStream output;
    }
}
