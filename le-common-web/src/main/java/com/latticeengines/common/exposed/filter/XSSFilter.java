package com.latticeengines.common.exposed.filter;

import org.springframework.stereotype.Component;

import java.io.IOException;
import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

@Component("xssSanitizeFilter")
public class XSSFilter implements Filter {
    static class XssRequestWrapper extends HttpServletRequestWrapper {
        /**
         * Policy file
         * Note that the policy files to be used need to be placed under the project resource file path
         * */
        private static String antiSamyPath = XssRequestWrapper.class.getClassLoader()
                .getResource( "antisamy-policy.xml").getFile();
        private static XSSSanitizer sanitizer = null;

        private String Sanitize(String value) {
            if (value != null) {
                if (null == sanitizer) {
                    sanitizer = new XSSSanitizer(antiSamyPath);
                }
                value = sanitizer.Sanitize(value);
            }
            return value;
        }

        public XssRequestWrapper(ServletRequest servletRequest) {
            super((HttpServletRequest)servletRequest);
        }

        @Override
        public String[] getParameterValues(String parameter) {
            String[] values = super.getParameterValues(parameter);

            if (values == null) {
                return null;
            }

            int count = values.length;
            String[] encodedValues = new String[count];
            for (int i = 0; i < count; i++) {
                encodedValues[i] = Sanitize(values[i]);
            }

            return encodedValues;
        }

        @Override
        public String getParameter(String parameter) {
            String value = super.getParameter(parameter);

            return Sanitize(value);
        }

        @Override
        public String getHeader(String name) {
            String value = super.getHeader(name);
            return Sanitize(value);
        }
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        chain.doFilter(new XssRequestWrapper(request), response);
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void destroy() {
    }
}
