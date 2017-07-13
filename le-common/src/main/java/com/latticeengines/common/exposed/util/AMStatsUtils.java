package com.latticeengines.common.exposed.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class AMStatsUtils {
    private static final Logger log = LoggerFactory.getLogger(AMStatsUtils.class);

    private static final ObjectMapper om = new ObjectMapper();

    public static String compressAndEncode(Object obj) throws IOException {
        byte[] bytes = GzipUtils.compress(om.writeValueAsString(obj));
        return Base64Utils.encodeBase64(bytes);
    }

    public static <T> T decompressAndDecode(String compressed, Class<T> clazz) throws IOException {
        byte[] bytes = Base64Utils.decodeBase64(compressed);
        String json = GzipUtils.decompress(bytes);
        T obj = om.readValue(json, clazz);
        return obj;
    }

    /**
     * This is command line tool to decode an encoded base64 string
     */
    public static void main(String[] args) throws ParseException,IOException {
        Option strOption = new Option("s", true, " encoded string");
        Option fileOption = new Option("f", true, " encoded string in file");

        Options options = new Options();
        options.addOption(strOption);
        options.addOption(fileOption);

        CommandLineParser parser = new PosixParser();
        CommandLine cmdLine = parser.parse(options, args);
        String str = "";
        if (options.hasOption("f")) {
            String filePath = cmdLine.getOptionValue("f");
            str = IOUtils.toString(new FileInputStream(new File(filePath)));
        } else {
            str = cmdLine.getOptionValue("s");
        }

        String cube = GzipUtils.decompress(Base64Utils.decodeBase64(str));
        System.out.println(cube);
    }
}
