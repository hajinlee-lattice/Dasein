package com.latticeengines.domain.exposed.playmaker;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public class PlaymakerUtils {

    private static final Logger log = LoggerFactory.getLogger(PlaymakerUtils.class);

    private static Random rand = new Random(System.currentTimeMillis());

    // this method uses best effort logic to deserialize contact info. In case
    // of any exception it simply returns empty list
    public static List<Map<String, String>> getExpandedContacts(String contacts) {
        return getExpandedContacts(contacts, String.class);
    }

    public static <T> List<Map<String, T>> getExpandedContacts(String contacts, Class<T> clazz) {
        List<Map<String, T>> contactList = null;

        if (!StringUtils.isBlank(contacts)) {
            try {
                List<?> contactListIntermediate1 = JsonUtils.deserialize(contacts, List.class);

                @SuppressWarnings("rawtypes")
                List<Map> contactListIntermediate2 = null;

                if (contactListIntermediate1.isEmpty()) {
                    contactListIntermediate2 = new ArrayList<>();
                } else {
                    contactListIntermediate2 = JsonUtils.convertList(contactListIntermediate1, Map.class);
                }

                if (!contactListIntermediate2.isEmpty()) {
                    contactList = contactListIntermediate2 //
                            .stream() //
                            .map(c -> JsonUtils.convertMap(c, String.class, clazz)) //
                            .collect(Collectors.toList());
                } else {
                    contactList = new ArrayList<>();
                }
            } catch (Exception ex) {
                log.warn("Ignoring exception while deseriazing contact data for the recommendation", ex);
            }
        }

        return contactList != null ? contactList : new ArrayList<>();
    }

    public static Date dateFromEpochSeconds(long unixTimestamp) {
        return new Date(TimeUnit.MILLISECONDS.convert(unixTimestamp, TimeUnit.SECONDS));
    }

    public static String convertToSFDCFieldType(String sourceLogicalDataType) {
        String type = sourceLogicalDataType;

        if (StringUtils.isNotBlank(sourceLogicalDataType)) {
            sourceLogicalDataType = sourceLogicalDataType.toLowerCase();

            if (sourceLogicalDataType.contains(PlaymakerConstants.VarChar)) {
                type = "nvarchar";
            } else if (sourceLogicalDataType.equals("double")) {
                type = "decimal";
            } else if (sourceLogicalDataType.equals("long")) {
                type = "bigint";
            } else if (sourceLogicalDataType.equals("boolean")) {
                type = "bit";
            }
        } else {
            type = "";
        }

        return type.toUpperCase();
    }

    public static Integer findLengthIfStringType(String sourceLogicalDataType) {
        Integer length = null;

        if (StringUtils.isNotBlank(sourceLogicalDataType)) {
            sourceLogicalDataType = sourceLogicalDataType.toLowerCase();

            if (sourceLogicalDataType.contains(PlaymakerConstants.VarChar)) {
                length = 4000;

                if (sourceLogicalDataType.contains("(")) {

                    sourceLogicalDataType = sourceLogicalDataType.substring(sourceLogicalDataType.indexOf("("));

                    if (sourceLogicalDataType.contains(")")) {

                        sourceLogicalDataType = sourceLogicalDataType.substring(0, sourceLogicalDataType.indexOf(")"));

                        if (StringUtils.isNumeric(sourceLogicalDataType)) {
                            length = Integer.parseInt(sourceLogicalDataType);
                        }
                    }
                }
            }
        }

        return length;
    }

    public static List<Map<String, String>> generateContactForRecommendation(List<Map<String, String>> rawContacts,
            boolean entityMatchEnabled) {
        List<Map<String, String>> contactsForRecommendation = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(rawContacts)) {
            rawContacts.stream().forEach(
                    rawContact -> processRawContact(rawContact, contactsForRecommendation, entityMatchEnabled));
        }
        return contactsForRecommendation;
    }

    private static void processRawContact(Map<String, String> rawContact,
            List<Map<String, String>> contactsForRecommendation, boolean entityMatchEnabled) {

        Map<String, String> contact = new HashMap<>();

        contact.put(PlaymakerConstants.Email, rawContact.get(InterfaceName.Email.name()));
        contact.put(PlaymakerConstants.Address, rawContact.get(InterfaceName.Address_Street_1.name()));
        contact.put(PlaymakerConstants.Phone, rawContact.get(InterfaceName.PhoneNumber.name()));
        contact.put(PlaymakerConstants.State, rawContact.get(InterfaceName.State.name()));
        contact.put(PlaymakerConstants.ZipCode, rawContact.get(InterfaceName.PostalCode.name()));
        contact.put(PlaymakerConstants.Country, rawContact.get(InterfaceName.Country.name()));
        contact.put(PlaymakerConstants.SfdcContactID, "");
        contact.put(PlaymakerConstants.City, rawContact.get(InterfaceName.City.name()));
        if (entityMatchEnabled) {
            contact.put(PlaymakerConstants.ContactID, rawContact.get(InterfaceName.CustomerContactId.name()));
        } else {
            contact.put(PlaymakerConstants.ContactID, rawContact.get(InterfaceName.ContactId.name()));
        }
        contact.put(PlaymakerConstants.Name, rawContact.get(InterfaceName.ContactName.name()));

        contactsForRecommendation.add(contact);

    }

    // TODO - remove it once we start getting contact data from redshift API
    public static List<Map<String, String>> createDummyContacts(String companyName) {
        List<Map<String, String>> contacts = new ArrayList<>();
        int randNum = rand.nextInt(10000);
        String firstName = "FirstName" + randNum;
        String lastName = "LastName" + randNum;

        Map<String, String> contact = new HashMap<>();
        String domain = createDummyDomain(companyName);

        contact.put(PlaymakerConstants.Email, firstName + "@" + domain);
        contact.put(PlaymakerConstants.Address, companyName + " Dr");
        contact.put(PlaymakerConstants.Phone, "248.813.2000");
        contact.put(PlaymakerConstants.State, "MI");
        contact.put(PlaymakerConstants.ZipCode, "48098-2815");
        contact.put(PlaymakerConstants.Country, "USA");
        contact.put(PlaymakerConstants.SfdcContactID, "");
        contact.put(PlaymakerConstants.City, "Troy");
        contact.put(PlaymakerConstants.ContactID, "" + randNum);
        contact.put(PlaymakerConstants.Name, firstName + " " + lastName);
        contacts.add(contact);
        return contacts;
    }

    private static String createDummyDomain(String companyName) {
        String dot = ".";
        int maxDomainLength = 7;
        String com = "com";

        String domain = "";

        if (companyName != null) {
            domain = companyName.trim();
            domain = StringUtils.replace(domain, dot + dot, dot);
            if (domain.endsWith(dot)) {
                domain = domain.substring(0, domain.length() - 1);
            }
            domain = StringUtils.replace(domain, " ", dot);
            domain = domain.replaceAll("[^A-Za-z0-9]", dot);
            domain = StringUtils.replace(domain, " ", dot);
            if (domain.endsWith(dot + com)) {
                if (domain.length() > maxDomainLength) {
                    domain = domain.substring(domain.length() - maxDomainLength, domain.length());
                }
                if (domain.startsWith(dot)) {
                    domain = domain.substring(dot.length());
                }
                return domain;
            } else if (!domain.endsWith(dot)) {
                domain += dot;
            }
        }
        domain = domain + com;
        if (domain.length() > maxDomainLength) {
            domain = domain.substring(domain.length() - maxDomainLength, domain.length());
        }
        if (domain.startsWith(dot)) {
            domain = domain.substring(dot.length());
        }
        return domain;
    }
}
