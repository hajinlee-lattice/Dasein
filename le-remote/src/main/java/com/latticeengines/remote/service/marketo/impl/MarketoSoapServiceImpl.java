package com.latticeengines.remote.service.marketo.impl;

import java.io.IOException;
import java.net.URL;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.namespace.QName;
import javax.xml.ws.WebServiceException;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.remote.marketo.LeadField;
import com.latticeengines.remote.exposed.service.marketo.MarketoSoapService;
import com.marketo.mktows.AuthenticationHeader;
import com.marketo.mktows.MObjFieldMetadata;
import com.marketo.mktows.MktMktowsApiService;
import com.marketo.mktows.MktowsPort;
import com.marketo.mktows.ParamsDescribeMObject;
import com.marketo.mktows.SuccessDescribeMObject;

@Component("marketoSoapService")
public class MarketoSoapServiceImpl implements MarketoSoapService {

    private static final Logger log = LoggerFactory.getLogger(MarketoSoapServiceImpl.class);
    private static final Marker fatal = MarkerFactory.getMarker("FATAL");

    @SuppressWarnings("unused")
    private static final ImmutableSet<String> EXCLUDED_DATATYPES = ImmutableSet.of("reference");
    private static final ImmutableSet<String> INCLUDED_DATATYPES = ImmutableSet.of("boolean", "currency", "date", "datetime",
            "email", "float", "integer", "phone", "string", "text", "url");

    @Override
    public boolean validateMarketoSoapCredentials(String soapEndPoint, String userId, String encryptionKey) {
        SuccessDescribeMObject lead = describeLeadObject(soapEndPoint, userId, encryptionKey);
        return lead.getResult().getMetadata().getFieldList().getFields().size() > 0;
    }

    @Override
    public List<LeadField> getLeadFields(String soapEndPoint, String userId, String encryptionKey) {
        List<LeadField> fields = new ArrayList<>();
        SuccessDescribeMObject lead = describeLeadObject(soapEndPoint, userId, encryptionKey);
        for (MObjFieldMetadata fieldMetadata : lead.getResult().getMetadata().getFieldList().getFields()) {
            if (INCLUDED_DATATYPES.contains(fieldMetadata.getDataType().toLowerCase())) {
                LeadField field = new LeadField();
                field.setDataType(fieldMetadata.getDataType().toLowerCase());
                field.setDisplayName(fieldMetadata.getDisplayName());
                field.setApiName(fieldMetadata.getSourceObject() + "." + fieldMetadata.getDisplayName());
                field.setSourceObject(fieldMetadata.getSourceObject());
                fields.add(field);
            }
        }
        return fields;
    }

    private SuccessDescribeMObject describeLeadObject(String soapEndPoint, String userId, String encryptionKey) {
        SuccessDescribeMObject result = null;
        String fullSoapEndPoint = soapEndPoint + "?WSDL";
        try {
            log.info("Creating marketo SOAP Request");
            URL soapUrl = new URL(fullSoapEndPoint);
            QName serviceName = new QName("http://www.marketo.com/mktows/", "MktMktowsApiService");
            MktMktowsApiService service = new MktMktowsApiService(soapUrl, serviceName);
            MktowsPort port = service.getMktowsApiSoapPort();

            log.info("Created marketo Service and Port");
            // Create Signature
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
            String text = df.format(new Date());
            String requestTimestamp = text.substring(0, 22) + ":" + text.substring(22);
            String encryptString = requestTimestamp + userId;

            SecretKeySpec secretKey = new SecretKeySpec(encryptionKey.getBytes(), "HmacSHA1");
            Mac mac = Mac.getInstance("HmacSHA1");
            mac.init(secretKey);
            byte[] rawHmac = mac.doFinal(encryptString.getBytes());
            char[] hexChars = Hex.encodeHex(rawHmac);
            String signature = new String(hexChars);

            // Set Authentication Header
            AuthenticationHeader header = new AuthenticationHeader();
            header.setMktowsUserId(userId);
            header.setRequestTimestamp(requestTimestamp);
            header.setRequestSignature(signature);

            // Create Request
            ParamsDescribeMObject request = new ParamsDescribeMObject();
            request.setObjectName("LeadRecord");

            log.info("Invoking describe Request");
            result = port.describeMObject(request, header);
            log.info("Got the Result: " + result);

            if (result == null) {
                throw new LedpException(LedpCode.LEDP_21035, new String[] { fullSoapEndPoint });
            }
        } catch (WebServiceException e) {
            throw new LedpException(LedpCode.LEDP_21035, new String[] { fullSoapEndPoint });
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_21034, new String[] { soapEndPoint });
        } catch (NoClassDefFoundError|ExceptionInInitializerError e) {
            log.warn("Marketo Describe Failure: ", e);
            throw new LedpException(LedpCode.LEDP_21036, new String[] { userId + " "+ encryptionKey, e.getMessage()});
        } catch (InvalidKeyException e) {
            log.error(fatal, "InvalidKeyException", e);
        } catch (NoSuchAlgorithmException e) {
            log.error(fatal, "NoSuchAlgorithmException", e);
        }

        return result;
    }

}
