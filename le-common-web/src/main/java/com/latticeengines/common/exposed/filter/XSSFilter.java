package com.latticeengines.common.exposed.filter;

import java.io.IOException;
import java.io.InputStream;

import javax.servlet.Filter;
import javax.servlet.ServletRequest;
import javax.servlet.FilterConfig;
import javax.servlet.ServletResponse;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.owasp.validator.html.PolicyException;
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
        private static String policyFile = "antisamy-policy.xml";

        private static boolean intialized = false;
        private static XSSSanitizer sanitizer = null;

        private static final Logger log = LoggerFactory.getLogger(XSSFilter.XssRequestWrapper.class);

        private String Sanitize(String value) {
            if (value != null) {
                if (!intialized) {
                    InitSanitizer();
                }

                if (sanitizer != null) {
                    value = sanitizer.Sanitize(value);
                }
            }
            return value;
        }

        private void InitSanitizer() {
            if (!intialized) {
                try {
                    InputStream policy = new ClassPathResource(policyFile).getInputStream();
                    sanitizer = new XSSSanitizer(policy);
                    log.info(String.format("Succeeded to initialize XSSSanitizer. AntiSamy Policy file: {%s}.", policyFile));
                } catch (IOException e) {
                    log.warn(String.format("Failed to initialize XSSSanitizer with policy file: {%s}. Please check this file's existence, and make sure it is accessible.", policyFile), e);
                } catch (PolicyException e) {
                    log.warn(String.format("Failed to initialize XSSSanitizer. Failed to load AntiSamy policy from: {%s}.", policyFile), e);
                }
                intialized = true;
            }
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
