package com.latticeengines.saml;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import org.opensaml.saml2.metadata.provider.MetadataProviderException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.saml.context.SAMLContextProviderImpl;
import org.springframework.security.saml.context.SAMLMessageContext;

import com.latticeengines.saml.util.SAMLUtils;

/**
 * Context provider which overrides request attributes with values of the
 * load-balancer or reverse-proxy in front of the local application. The
 * settings help to provide correct redirect URls and verify destination URLs
 * during SAML processing.
 */
public class LatticeSAMLContextProviderLB extends SAMLContextProviderImpl {

    @Value("${saml.lb.scheme:http}")
    private String scheme;

    @Value("${saml.lb.servername:localhost}")
    private String serverName;

    @Value("${saml.lb.includeserverport:true}")
    private boolean includeServerPortInRequestURL;

    @Value("${saml.lb.serverport:8081}")
    private int serverPort;

    @Value("${saml.lb.contextpath:/pls/saml/login/}")
    private String contextPath;

    /**
     * Method wraps the original request and provides values specified for
     * load-balancer. The following methods are overriden: getContextPath,
     * getRequestURL, getRequestURI, getScheme, getServerName, getServerPort and
     * isSecure.
     *
     * @param request
     *            original request
     * @param response
     *            response object
     * @param context
     *            context to populate values to
     */
    @Override
    protected void populateGenericContext(HttpServletRequest request, HttpServletResponse response,
            SAMLMessageContext context) throws MetadataProviderException {

        super.populateGenericContext(new LPRequestWrapper(request), response, context);

    }

    /**
     * Wrapper for original request which overrides values of scheme, server
     * name, server port and contextPath. Method isSecure returns value based on
     * specified scheme.
     */
    private class LPRequestWrapper extends HttpServletRequestWrapper {

        private LPRequestWrapper(HttpServletRequest request) {
            super(request);
        }

        @Override
        public String getContextPath() {
            return contextPath;
        }

        @Override
        public String getScheme() {
            return scheme;
        }

        @Override
        public String getServerName() {
            return serverName;
        }

        @Override
        public int getServerPort() {
            return serverPort;
        }

        @Override
        public String getRequestURI() {
            StringBuilder sb = new StringBuilder(contextPath);
            sb.append(getServletPath());
            return sb.toString();
        }

        @Override
        public StringBuffer getRequestURL() {
            StringBuffer sb = new StringBuffer();
            sb.append(scheme).append("://").append(serverName);
            if (includeServerPortInRequestURL)
                sb.append(":").append(serverPort);
            sb.append(contextPath);
            if (getPathInfo() != null) {
                String tenantId = SAMLUtils.getTenantFromAlias(getPathInfo());
                sb.append(tenantId);
            }

            return sb;
        }

        @Override
        public boolean isSecure() {
            return "https".equalsIgnoreCase(scheme);
        }

    }
}
