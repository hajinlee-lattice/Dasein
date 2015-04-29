package com.latticeengines.dellebi.routebuilder;

import java.io.InputStream;
import java.io.StringWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.dataformat.zipfile.ZipFileDataFormat;
import org.apache.camel.dataformat.zipfile.ZipSplitter;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.dellebi.flowdef.DailyFlow;

public class FileUnzipRouteBuilder extends RouteBuilder {

    private static final Log log = LogFactory.getLog(FileUnzipRouteBuilder.class);

    @Value("${dellebi.camelunzipoutputpath}")
    private String camelUnzipOutputPath;

    @Value("${dellebi.ordersummary}")
    private String orderSummary;
    @Value("${dellebi.orderdetail}")
    private String orderDetail;
    @Value("${dellebi.shiptoaddrlattice}")
    private String shipToAddrLattice;
    @Value("${dellebi.warrantyglobal}")
    private String warrantyGlobal;
    @Value("${dellebi.quotetrans}")
    private String quoteTrans;
    
    @Value("${dellebi.quoteheaders}")
    private String quoteHeaders;

    @Value("${dellebi.smbaccount}")
    private String smbAccount;
    @Value("${dellebi.smbps}")
    private String smbPS;
    @Value("${dellebi.smbinboxpath}")
    private String smbInboxPath;

    @SuppressWarnings("unchecked")
    public void configure() {
        // Unzip files to separated folders according to the file type.
        log.info("Unzipping files and put files to seperated file folders according to file type.");
        NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", smbAccount, smbPS);

        final NtlmPasswordAuthentication finalAuth = auth;

        ZipFileDataFormat zipFile = new ZipFileDataFormat();
        zipFile.setUsingIterator(true);

        from("direct:files")
                .routeId("CopyFileToFolders")
                .choice()
                .when(header("CamelFileName").startsWith("tgt_lat_order_summary_global"))
                .split(new ZipSplitter())
                .streaming()
                .to("mock:processZipEntry")
                .to(camelUnzipOutputPath + "/" + orderSummary + "/")
                .process(new Processor() {
                    public void process(Exchange exchange) throws Exception {
                        String fileName = smbInboxPath + "/" + exchange.getIn().getHeader("CamelFileName").toString();

                        if (exchange.getIn().getHeader("CamelFileName").toString() != "") {
                            log.info("Removing Dell EBI file: " + fileName.substring(0, fileName.indexOf(".txt"))
                                    + ".zip");
                            if (!System.getProperty("DELLEBI_PROPDIR").contains("conf/env/local")) {
                                SmbFile smbFile = new SmbFile(fileName.substring(0, fileName.indexOf(".txt")) + ".zip",
                                        finalAuth);
                                if (smbFile.canWrite()) {
                                    smbFile.delete();
                                }
                            }
                        }
                    }
                })
                .endChoice()
                .when(header("CamelFileName").startsWith("tgt_order_detail_global"))
                .split(new ZipSplitter())
                .streaming()
                .to("mock:processZipEntry")
                .to(camelUnzipOutputPath + "/" + orderDetail + "/")
                .process(new Processor() {
                    public void process(Exchange exchange) throws Exception {
                        String fileName = smbInboxPath + "/" + exchange.getIn().getHeader("CamelFileName");

                        if (exchange.getIn().getHeader("CamelFileName").toString() != "") {
                            log.info("Removing Dell EBI file: " + fileName.substring(0, fileName.indexOf(".txt"))
                                    + ".zip");
                            if (!System.getProperty("DELLEBI_PROPDIR").contains("conf/env/local")) {
                                SmbFile smbFile = new SmbFile(fileName.substring(0, fileName.indexOf(".txt")) + ".zip",
                                        finalAuth);
                                if (smbFile.canWrite()) {
                                    smbFile.delete();
                                }
                            }
                        }
                    }
                })
                .endChoice()
                .when(header("CamelFileName").startsWith("tgt_ship_to_addr_lattice"))
                .split(new ZipSplitter())
                .streaming()
                .to("mock:processZipEntry")
                .to(camelUnzipOutputPath + "/" + shipToAddrLattice + "/")
                .process(new Processor() {
                    public void process(Exchange exchange) throws Exception {
                        String fileName = smbInboxPath + "/" + exchange.getIn().getHeader("CamelFileName");

                        if (exchange.getIn().getHeader("CamelFileName").toString() != "") {
                            log.info("Removing Dell EBI file: " + fileName.substring(0, fileName.indexOf(".txt"))
                                    + ".zip");
                            if (!System.getProperty("DELLEBI_PROPDIR").contains("conf/env/local")) {
                                SmbFile smbFile = new SmbFile(fileName.substring(0, fileName.indexOf(".txt")) + ".zip",
                                        finalAuth);
                                if (smbFile.canWrite()) {
                                    smbFile.delete();
                                }
                            }
                        }
                    }
                })
                .endChoice()
                .when(header("CamelFileName").startsWith("tgt_quote_trans_global_1"))
                .split(new ZipSplitter())
                .streaming()
                .to("mock:processZipEntry")
                .to(camelUnzipOutputPath + "/" + quoteTrans + "/")
                .process(new Processor() {
                    public void process(Exchange exchange) throws Exception {
                        String fileName = smbInboxPath + "/" + exchange.getIn().getHeader("CamelFileName");

                        if (exchange.getIn().getHeader("CamelFileName").toString() != "") {
                            log.info("Removing Dell EBI file: " + fileName.substring(0, fileName.indexOf(".txt"))
                                    + ".zip");
                            if (!System.getProperty("DELLEBI_PROPDIR").contains("conf/env/local")) {
                                SmbFile smbFile = new SmbFile(fileName.substring(0, fileName.indexOf(".txt")) + ".zip",
                                        finalAuth);
                                if (smbFile.canWrite()) {
                                    smbFile.delete();
                                }
                            }
                        }
                    }
                })
                .endChoice()
                .when(header("CamelFileName").startsWith("tgt_quote_trans_global"))
                .split(new ZipSplitter())
                .streaming()
                .to("mock:processZipEntry")
                .process(new Processor() {
							public void process(Exchange exchange) throws Exception {
								
								Message in = exchange.getIn();
								
								StringWriter writer = new StringWriter();
								IOUtils.copy(exchange.getIn().getBody(InputStream.class), writer);
								String theString = writer.toString();
								
								in.setBody(quoteHeaders + "\n" + theString);
							}
						})
                .to(camelUnzipOutputPath + "/" + quoteTrans + "/")
                .process(new Processor() {
                    public void process(Exchange exchange) throws Exception {
                        String fileName = smbInboxPath + "/" + exchange.getIn().getHeader("CamelFileName");

                        if (exchange.getIn().getHeader("CamelFileName").toString() != "") {
                            log.info("Removing Dell EBI file: " + fileName.substring(0, fileName.indexOf(".txt"))
                                    + ".zip");
                            if (!System.getProperty("DELLEBI_PROPDIR").contains("conf/env/local")) {
                                SmbFile smbFile = new SmbFile(fileName.substring(0, fileName.indexOf(".txt")) + ".zip",
                                        finalAuth);
                                if (smbFile.canWrite()) {
                                    smbFile.delete();
                                }
                            }
                        }
                    }
                })
                .endChoice()
                .when(header("CamelFileName").startsWith("tgt_warranty_global")).split(new ZipSplitter())
                .streaming().to("mock:processZipEntry").to(camelUnzipOutputPath + "/" + warrantyGlobal + "/")
                .process(new Processor() {
                    public void process(Exchange exchange) throws Exception {
                        String fileName = smbInboxPath + "/" + exchange.getIn().getHeader("CamelFileName");

                        if (exchange.getIn().getHeader("CamelFileName").toString() != "") {
                            log.info("Adding headers to Dell EBI file: " + fileName);
                            if (!System.getProperty("DELLEBI_PROPDIR").contains("conf/env/local")) {
                                SmbFile smbFile = new SmbFile(fileName.substring(0, fileName.indexOf(".txt")) + ".zip",
                                        finalAuth);
                                if (smbFile.canWrite()) {
                                    smbFile.delete();
                                }
                            }
                        }
                    }
                }).end();
    }
}
