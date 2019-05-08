package com.latticeengines.spark.service.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.ScriptJobConfig;
import com.latticeengines.domain.exposed.spark.SparkInterpreter;

import reactor.core.publisher.Mono;

class SparkScriptClient {

    private static final Logger log = LoggerFactory.getLogger(SparkScriptClient.class);

    private static final String URI_STATEMENTS = "/statements";
    private static final String BEGIN_OUTPUT = "----- BEGIN SCRIPT OUTPUT -----";
    private static final String END_OUTPUT = "----- END SCRIPT OUTPUT -----";

    private final WebClient webClient;
    private final SparkInterpreter interpreter;
    private final ObjectMapper om = new ObjectMapper();

    SparkScriptClient(SparkInterpreter interpreter, String url) {
        this.interpreter = interpreter;
        webClient = WebClient.builder() //
                .baseUrl(url) //
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE) //
                .build();
    }

    void runPreScript(ScriptJobConfig config) {
        List<HdfsDataUnit> targets = config.getTargets();
        if (targets == null) {
            targets = new ArrayList<>();
        }
        List<DataUnit> input = config.getInput();
        if (input == null) {
            input = new ArrayList<>();
        }
        JsonNode params = config.getParams();
        if (params == null) {
            params = om.createObjectNode();
        }
        String workspace = config.getWorkspace();
        String checkpointDir = "/spark-checkpoints";
        if (StringUtils.isNotBlank(workspace)) {
            checkpointDir = workspace + File.separator + "checkpoints";
        }
        String statement = getInitializeTemplate( //
                JsonUtils.serialize(targets), //
                JsonUtils.serialize(input), //
                JsonUtils.serialize(params),
                checkpointDir);
        runStatement(statement);
        log.info("Script env initialized.");
    }

    String printOutputStr() {
        String statement = getPrintOutTemplate();
        return runStatement(statement);
    }

    List<HdfsDataUnit> runPostScript() {
        String statement = getFinalizeTemplate();
        String result = runStatement(statement);
        log.info("Script env finalized.");
        try {
            return om.readValue(result, new TypeReference<List<HdfsDataUnit>>() {
            });
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse result to List<SparkJobTarget>.", e);
        }
    }

    String runStatement(String statement) {
        int id = submitStatement(statement);
        String statementPrint = waitStatementOutput(id).block();
        if (StringUtils.isNotBlank(statementPrint)) {
            log.info("Statement " + id + " prints: " + statementPrint);
        } else {
            log.info("Statement " + id + " is finished.");
        }
        return statementPrint;
    }

    private int submitStatement(String statement) {
        Map<String, Object> body = new HashMap<>();
        body.put("code", statement);
        body.put("kind", interpreter.getKind());
        JsonNode response = webClient //
                .method(HttpMethod.POST) //
                .uri(URI_STATEMENTS) //
                .syncBody(body) //
                .retrieve() //
                .bodyToMono(JsonNode.class) //
                .block();
        if (response == null) {
            throw new RuntimeException("Empty response of submitting the statement: " + statement);
        }
        return response.get("id").asInt();
    }

    private Mono<String> waitStatementOutput(int id) {
        return Mono.fromCallable(() -> {
            LivyStatement statement = getStatement(id);
            while(!LivyStatement.State.TERMINAL_STATES.contains(statement.state)) {
                Thread.sleep(10000L);
                statement = getStatement(id);
                log.info("Statement " + id +" is " + statement.state //
                        + " - " + statement.progress * 100);
            }
            if (LivyStatement.State.available.equals(statement.state)) {
                if ("error".equals(statement.output.status)) {
                    throw new RuntimeException("Statement " + id + " failed with " + statement.output.ename + " : " + statement.output.evalue);
                } else {
                    JsonNode json = statement.output.data;
                    String output = json.get("text/plain").asText();
                    return parseOutput(output);
                }
            } else {
                throw new RuntimeException("Statement " + id + " ends with " + statement.state);
            }
        });
    }

    private LivyStatement getStatement(int id) {
        String resp = webClient //
                .method(HttpMethod.GET) //
                .uri(URI_STATEMENTS + "/" + id) //
                .retrieve() //
                .bodyToMono(String.class) //
                .block();
        try {
            return om.readValue(resp, LivyStatement.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed parse response: " + resp, e);
        }
    }

    private String getInitializeTemplate(String targets, String input, String params, String checkpointDir) {
        return getTemplate("initialize", ImmutableMap.of( //
                "{{TARGETS}}", targets, //
                "{{INPUT}}", input, //
                "{{PARAMS}}", params, //
                "{{CHECKPOINT_DIR}}", checkpointDir //
        ));
    }

    private String getPrintOutTemplate() {
        return getTemplate("printout", Collections.emptyMap());
    }

    private String getFinalizeTemplate() {
        return getTemplate("finalize", Collections.emptyMap());
    }

    private String getTemplate(String template, Map<String, String> replace) {
        String resource = "scripts/" + template +".";
        switch (interpreter) {
            case Scala:
                resource += "scala";
                break;
            case Python:
                resource += "py";
                break;
            default:
                throw new UnsupportedOperationException("Unknown interpreter " + interpreter);
        }
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
        String result;
        try {
            result = IOUtils.toString(is, Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException("Failed to read " + resource, e);
        }
        for (Map.Entry<String, String> entry: replace.entrySet()) {
            String token = entry.getKey();
            String value = entry.getValue();
            result = result.replace(token, value);
        }
        return result;
    }

    private String parseOutput(String text) {
        List<String> outputParagraphs = new ArrayList<>();
        while (text.contains(BEGIN_OUTPUT)) {
            text = text.substring(text.indexOf(BEGIN_OUTPUT) + BEGIN_OUTPUT.length());
            String output;
            if (text.contains(END_OUTPUT)) {
                output = text.substring(0, text.indexOf(END_OUTPUT));
                text = text.substring(text.indexOf(END_OUTPUT) + END_OUTPUT.length());
            } else {
                output = text;
                text = "";
            }
            if (output.endsWith("\n")) {
                output = output.substring(0, output.lastIndexOf("\n"));
            }
            outputParagraphs.add(output);
        }
        return StringUtils.join(outputParagraphs, "\n");
    }

}
