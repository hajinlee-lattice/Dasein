package com.latticeengines.release.jenkins.xml.helper;

import java.io.StringReader;
import java.io.StringWriter;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class JenkinsXMLHelper {

    public static String convertXMLDocumentToString(Document document) {
        try {
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            StringWriter sw = new StringWriter();
            StreamResult result = new StreamResult(sw);

            DOMSource source = new DOMSource(document);
            transformer.transform(source, result);
            return sw.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Document updateVersionInXMLDocument(String version, String configuration) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(new InputSource(new StringReader(configuration)));

            Element element = document.getDocumentElement();
            NodeList nodeList = element.getElementsByTagName("hudson.model.StringParameterDefinition");
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node parameterDefinitionNode = nodeList.item(i);
                for (int j = 0; j < parameterDefinitionNode.getChildNodes().getLength(); j++) {
                    Node nameNode = parameterDefinitionNode.getChildNodes().item(j);
                    if (nameNode.getNodeName().equals("name")
                            && nameNode.getFirstChild().getNodeValue().equals("SVN_BRANCH_NAME")) {
                        Node nextSiblingNode = nameNode.getNextSibling();
                        while (!nextSiblingNode.getNodeName().equals("defaultValue")) {
                            nextSiblingNode = nextSiblingNode.getNextSibling();
                        }
                        nextSiblingNode.getFirstChild().setNodeValue(version);
                        return document;
                    }
                }
            }
            return document;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
