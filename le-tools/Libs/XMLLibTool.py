__author__ = 'nxu'

import xpath
import xml.dom.minidom as mdom
from xml.dom.minidom import getDOMImplementation, Text, parseString
import codecs

class XmlToolLibrary(object):

    dom = ""
    doc = ""

    def loadXML(self, url):
        self.dom = mdom.parse(url)
        self.doc = self.dom.documentElement

    def loadXMLFromString(self, xml_str):
        self.dom = mdom.parseString(xml_str)
        self.doc = self.dom.documentElement

    def saveXMLFile(self, url):
        f=file(url, 'w')
        writer = codecs.lookup('utf-8')[3](f)
        self.dom.writexml(writer, encoding='utf-8')
        writer.close()

    def getxmlVal(self, expr):
        data = xpath.findvalue(expr, self.doc)
        return data

    def getxmlVals(self, expr):
        data = xpath.findvalues(expr, self.doc)
        return data

    def getstandlone(self):
        data = self.dom._get_standalone()
        return data

    def getValFromXmlString(self, expr, xmlStr):
        parsedXmlStr=parseString(xmlStr)
        data = xpath.findvalue(expr, parsedXmlStr)
        return data

    def getValsFromXmlString(self, expr, xmlStr):
        parsedXmlStr=parseString(xmlStr)
        data = xpath.findvalues(expr, parsedXmlStr)
        return data

    def setxmlAtt(self, expr, attName, attValues):
        node = xpath.findnode(expr, self.doc)
        node.setAttribute(attName,attValues)

    def getxmlAtt(self, expr, attName):
        node = xpath.findnode(expr, self.doc)
        attValue = node.getAttribute(attName)
        return attValue

    def setxmlAtts(self, expr, attName, attValues):
        nodes = xpath.find(expr, self.doc)
        for node in nodes:
            node.setAttribute(attName,attValues)


    def replaceAtt(self, expr, *attdata):
        nodes = xpath.find(expr, self.doc)
        attNames = attdata[0]
        attValues = attdata[1]
        for elemment in nodes:
            for index in range(len(attNames)):
                elemment.setAttribute(attNames[index], attValues[index])


    def replaceText(self, expr, textval):
        nodes = xpath.find(expr, self.doc)
        for elemment in nodes:
            for text in elemment.childNodes:
                if text.nodeType is Text.TEXT_NODE:
                    text.replaceWholeText(textval)
                    break

            if len(elemment.childNodes) == 0:
                for elemment in nodes:
                    impl = getDOMImplementation()
                    newdoc = impl.createDocument(None, "test", None)
                    text = newdoc.createTextNode(textval)
                    elemment.appendChild(text)

    def replaceTextWithNamespace(self, expr, textval,namespace):
        context = xpath.XPathContext(default_namespace=namespace)
        nodes = context.find(expr, self.doc)
        for elemment in nodes:
            for text in elemment.childNodes:
                if text.nodeType is Text.TEXT_NODE:
                    text.replaceWholeText(textval)
                    break

            if len(elemment.childNodes) == 0:
                for elemment in nodes:
                    impl = getDOMImplementation()
                    newdoc = impl.createDocument(None, "test", None)
                    text = newdoc.createTextNode(textval)
                    elemment.appendChild(text)

    def removeXmlElement(self, expr):
        node = xpath.findnode(expr, self.doc)
        node.parentNode.removeChild(node)

    def removeXmlAtt(self, expr, attName):
        node = xpath.findnode(expr, self.doc)
        node.removeAttribute(attName)


    def addXmlElement(self, expr, tagName, textValue, *attdata):
        if(len(attdata)>0):
            attNames = attdata[0]
            attValues = attdata[1]
        else:
            attNames = ""
            attValues = ""
        node = xpath.findnode(expr, self.doc)
        x = self._addXmlAttAndText(tagName, attNames, attValues, textValue)
        node.appendChild(x)

    def _addXmlAttAndText(self, tagName, attNames, attValues, textValue):
        impl = getDOMImplementation()
        newdoc = impl.createDocument(None, tagName, None)
        top_element = newdoc.documentElement
        text = newdoc.createTextNode(textValue)
        top_element.appendChild(text)

        for index in range(len(attNames)):
            top_element.setAttribute(attNames[index], attValues[index])
        if(textValue != None):
            top_element.appendChild(text)
        return top_element

    def removeElementsWithNamspaces(self, expr, namespace):
        context = xpath.XPathContext(default_namespace=namespace)
        nodelist = context.find(expr, self.doc)
        for nodeitem in nodelist:
            nodeitem.parentNode.removeChild(nodeitem)

    def getValNamespaces(self, expr, namespace):
        context = xpath.XPathContext(default_namespace=namespace)
        return context.findvalue(expr, self.doc);

    def getValsNamespaces(self, expr, namespace):
        context = xpath.XPathContext(default_namespace=namespace)
        return context.findvalues(expr, self.doc);
