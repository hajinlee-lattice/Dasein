package com.latticeengines.saml;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.opensaml.saml2.metadata.provider.AbstractMetadataProvider;
import org.opensaml.saml2.metadata.provider.MetadataProvider;
import org.opensaml.saml2.metadata.provider.MetadataProviderException;
import org.opensaml.xml.XMLObject;
import org.opensaml.xml.parse.ParserPool;
import org.springframework.security.saml.metadata.ExtendedMetadata;
import org.springframework.security.saml.metadata.ExtendedMetadataDelegate;

import com.latticeengines.domain.exposed.saml.IdentityProvider;

public class IdentityProviderMetadataAdaptor extends ExtendedMetadataDelegate {

    public IdentityProviderMetadataAdaptor(ParserPool parserPool, IdentityProvider underlying) {
        super(getMetadataProvider(parserPool, underlying), getExtendedMetadata(underlying));
    }

    private static MetadataProvider getMetadataProvider(final ParserPool pool, final IdentityProvider underlying) {
        return new AbstractMetadataProvider() {
            @Override
            protected XMLObject doGetMetadata() throws MetadataProviderException {
                setParserPool(pool);
                try (InputStream is = new ByteArrayInputStream(underlying.getMetadata().getBytes("UTF-8"))) {
                    return unmarshallMetadata(is);
                } catch (Exception e) {
                    throw new MetadataProviderException("Failure creating IdentityProvider", e);
                }
            }
        };
    }

    private static ExtendedMetadata getExtendedMetadata(final IdentityProvider underlying) {
        ExtendedMetadata extendedMetadata = new ExtendedMetadata();
        extendedMetadata.setAlias(underlying.getGlobalAuthTenant().getId());
        extendedMetadata.setSecurityProfile("metaiop");
        extendedMetadata.setSslSecurityProfile("pkix");
        extendedMetadata.setSslHostnameVerification("default");
        extendedMetadata.setRequireLogoutRequestSigned(true);
        extendedMetadata.setRequireArtifactResolveSigned(true);
        extendedMetadata.setSupportUnsolicitedResponse(true);
        return extendedMetadata;
    }

}
