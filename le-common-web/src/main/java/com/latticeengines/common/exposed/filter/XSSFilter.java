package com.latticeengines.common.exposed.filter;

import java.io.IOException;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

@Component("xssSanitizeFilter")
public class XSSFilter implements Filter {
    static class XssRequestWrapper extends HttpServletRequestWrapper {
        /**
         * Policy file
         * Note that the policy files to be used need to be placed under the project resource file path
         * */
        private static boolean intialized = false;
        private static String policyFile = "antisamy-policy.xml";
        private static XSSSanitizer sanitizer = null;
        private static final Logger log = LoggerFactory.getLogger(XSSFilter.XssRequestWrapper.class);

        private String Sanitize(String value) {
            if (value != null) {
                if (!intialized) {
                    try {
                        String antiSamyPath = new ClassPathResource(policyFile).getFile().getPath();
                        sanitizer = new XSSSanitizer(antiSamyPath);
                    } catch (IOException e) {
                        log.warn(String.format("Filed to open AntiSamy policy file: {%s}", policyFile), e);
                    }
                    intialized = true;
                }

                if (sanitizer != null) {
                    value = sanitizer.Sanitize(value);
                }
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
