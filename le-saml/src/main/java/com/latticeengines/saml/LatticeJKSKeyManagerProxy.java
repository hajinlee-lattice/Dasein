package com.latticeengines.saml;

import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.opensaml.xml.security.CriteriaSet;
import org.opensaml.xml.security.SecurityException;
import org.opensaml.xml.security.credential.Credential;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.io.Resource;
import org.springframework.security.saml.key.JKSKeyManager;
import org.springframework.security.saml.key.KeyManager;
import org.springframework.stereotype.Component;

@Component("latticeJKSKeyManagerProxy")
public class LatticeJKSKeyManagerProxy implements KeyManager, ApplicationContextAware {

    @Value("${saml.keystore.resource.path.encrypted}")
    private volatile String keyStorePath;

    @Value("${saml.keystore.defaultKey.encrypted}")
    private volatile String defaultKey;

    @Value("${saml.keystore.password.encrypted}")
    private volatile String password;

    @Value("${saml.keystore.storePass.encrypted}")
    private volatile String storePass;

    private JKSKeyManager keyManager;

    private ApplicationContext applicationContext;

    @PostConstruct
    public void initiate() {
        Map<String, String> passwords = new HashMap<>();
        passwords.put(defaultKey, password);
        Resource storeFile = applicationContext.getResource(keyStorePath);
        keyManager = new JKSKeyManager(storeFile, storePass, passwords, defaultKey);
    }

    @Override
    public Iterable<Credential> resolve(CriteriaSet criteria) throws SecurityException {
        return keyManager.resolve(criteria);
    }

    @Override
    public Credential resolveSingle(CriteriaSet criteria) throws SecurityException {
        return keyManager.resolveSingle(criteria);
    }

    @Override
    public Credential getCredential(String keyName) {
        return keyManager.getCredential(keyName);
    }

    @Override
    public Credential getDefaultCredential() {
        return keyManager.getDefaultCredential();
    }

    @Override
    public String getDefaultCredentialName() {
        return keyManager.getDefaultCredentialName();
    }

    @Override
    public Set<String> getAvailableCredentials() {
        return keyManager.getAvailableCredentials();
    }

    @Override
    public X509Certificate getCertificate(String alias) {
        return keyManager.getCertificate(alias);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
