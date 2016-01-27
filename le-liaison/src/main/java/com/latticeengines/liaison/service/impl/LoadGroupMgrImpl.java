package com.latticeengines.liaison.service.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import com.latticeengines.liaison.exposed.exception.UnknownDataLoaderObjectException;
import com.latticeengines.liaison.exposed.service.ConnectionMgr;
import com.latticeengines.liaison.exposed.service.LoadGroupMgr;

public class LoadGroupMgrImpl implements LoadGroupMgr {

    private static final Set<String> functionalities = new HashSet<String>(Arrays.asList("schemas",
            "ngs", "visiDBConfigurationWithMacros", "targetQueries", "targetQuerySequences", "rdss",
            "validationExtracts", "ces", "extractQueries", "extractQuerySequences", "leafExtracts",
            "launchExtracts", "jobs", "pdmatches", "leadscroings", "lssbardins", "lssbardouts",
            "lds", "ecs", "gCs"));

    private final ConnectionMgr conn_mgr;
    private Document doc;
    private Transformer transformer;

    public LoadGroupMgrImpl(ConnectionMgr conn_mgr, String configfile) throws RuntimeException {
        this.conn_mgr = conn_mgr;
        initFromConfig(configfile);
    }

    @Override
    public Boolean hasLoadGroup(String groupName) {
        Element groups = (Element) doc.getDocumentElement().getElementsByTagName("groups").item(0);
        for (int i = 0; i < groups.getElementsByTagName("group").getLength(); i++) {
            Element group = (Element) groups.getElementsByTagName("group").item(i);
            if (group.getAttribute("name").equalsIgnoreCase(groupName)) {
                return Boolean.TRUE;
            }
        }
        return Boolean.FALSE;
    }

    @Override
    public String getLoadGroupFunctionalityXML(String groupName, String functionality)
            throws UnknownDataLoaderObjectException, TransformerException {

        Element f = getLoadGroupFunctionality(groupName, functionality);
        StreamResult result = new StreamResult(new StringWriter());
        DOMSource source = new DOMSource(f);
        transformer.transform(source, result);
        return result.getWriter().toString();
    }

    @Override
    public Element getLoadGroupFunctionality(String groupName, String functionality)
            throws UnknownDataLoaderObjectException {

        if (!hasLoadGroup(groupName)) {
            throw new UnknownDataLoaderObjectException(String.format("Unknown LoadGroup: %s", groupName));
        }

        if (!functionalities.contains(functionality)) {
            throw new UnknownDataLoaderObjectException(String.format("Unknown DataLoader functionality: %s", functionality));
        }

        Element group = doc.createElement("group");
        Element groups = (Element) doc.getDocumentElement().getElementsByTagName("groups").item(0);
        for (int i = 0; i < groups.getElementsByTagName("group").getLength(); i++) {
            Element aGroup = (Element) groups.getElementsByTagName("group").item(i);
            if (aGroup.getAttribute("name").equalsIgnoreCase(groupName)) {
                group = aGroup;
                break;
            }
        }
        if (group.getElementsByTagName(functionality).getLength() > 0) {
            Element f = (Element) group.getElementsByTagName(functionality).item(0);
            return f;
        }
        return doc.createElement(functionality);
    }

    @Override
    public void setLoadGroupFunctionalityXML(String groupName, String xmlConfig)
            throws UnknownDataLoaderObjectException, RuntimeException {

        if (!hasLoadGroup(groupName)) {
            throw new UnknownDataLoaderObjectException(String.format("Unknown LoadGroup: %s", groupName));
        }

        DocumentBuilder builder;
        Document fcndoc;
        ByteArrayInputStream xml;

        try {
            builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            xml = new ByteArrayInputStream(xmlConfig.getBytes("UTF-8"));
            fcndoc = builder.parse(xml);
        }
        catch (ParserConfigurationException|SAXException|IOException ex) {
            throw new RuntimeException(String.format("Exception when building Document from XML: %s", ex.getMessage()));
        }

        Element elem = fcndoc.getDocumentElement();
        setLoadGroupFunctionality(groupName, elem);
    }

    @Override
    public void setLoadGroupFunctionality(String groupName, Element elem)
            throws UnknownDataLoaderObjectException {

        if (!hasLoadGroup(groupName)) {
            throw new UnknownDataLoaderObjectException(String.format("Unknown LoadGroup: %s", groupName));
        }

        String functionality = elem.getTagName();
        if (!functionalities.contains(functionality)) {
            throw new UnknownDataLoaderObjectException(String.format("Unknown DataLoader functionality: %s", functionality));
        }

        Element group = doc.createElement("group");
        Element groups = (Element) doc.getDocumentElement().getElementsByTagName("groups").item(0);
        for (int i = 0; i < groups.getElementsByTagName("group").getLength(); i++) {
            Element aGroup = (Element) groups.getElementsByTagName("group").item(i);
            if (aGroup.getAttribute("name").equalsIgnoreCase(groupName)) {
                group = aGroup;
                break;
            }
        }
        if (group.getElementsByTagName(functionality).getLength() > 0) {
            Element f_old = (Element) group.getElementsByTagName(functionality).item(0);
            group.replaceChild(elem, f_old);
        }
        else {
            group.appendChild(elem);
        }
    }

    @Override
    public void commit() throws IOException, RuntimeException {
        this.conn_mgr.installDLConfigFile(getConfig());
    }

    public String getConfig() {
        StreamResult result = new StreamResult(new StringWriter());
        DOMSource source = new DOMSource(doc);
        try {
            transformer.transform(source, result);
        }
        catch (TransformerException ex) {
            throw new RuntimeException(String.format("Exception when transforming Document to XML: %s", ex.getMessage()));
        }
        return result.getWriter().toString();
    }

    private void initFromConfig(String configstr) throws RuntimeException {

        DocumentBuilder builder;
        ByteArrayInputStream xml;

        try {
            builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            xml = new ByteArrayInputStream(configstr.getBytes("UTF-8"));
            this.doc = builder.parse(xml);
            this.transformer = TransformerFactory.newInstance().newTransformer();
        }
        catch (ParserConfigurationException|SAXException|TransformerException|IOException ex) {
            throw new RuntimeException(String.format("Exception when building Document from XML: %s", ex.getMessage()));
        }

        Element dataProviders = (Element) doc.getDocumentElement().getElementsByTagName("dataProviders").item(0);
        Element dataProvidersEmpty = doc.createElement("dataProviders");
        doc.getDocumentElement().replaceChild(dataProvidersEmpty, dataProviders);
    }
}
