package com.latticeengines.spark.service.impl;

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
import org.apache.commons.text.StringEscapeUtils;
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
        String statement = getInitializeTemplate( //
                JsonUtils.serialize(targets), //
                JsonUtils.serialize(input), //
                JsonUtils.serialize(params));
        String result = runStatement(statement);
        log.info("Initialized:\n" + result);
    }

    List<HdfsDataUnit> runPostScript() {
        String statement = getFinalizeTemplate();
        String result = runStatement(statement);
        log.info("Finalize:\n" + result);
        try {
            return om.readValue(result, new TypeReference<List<HdfsDataUnit>>() {
            });
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse result to List<SparkJobTarget>.", e);
        }
    }

    String runStatement(String statement) {
        int id = submitStatement(statement);
        return waitStatementOutput(id).block();
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
                        + " - " + String.valueOf(statement.progress * 100));
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
                .uri(URI_STATEMENTS + "/" + String.valueOf(id)) //
                .retrieve() //
                .bodyToMono(String.class) //
                .block();
        try {
            return om.readValue(resp, LivyStatement.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed parse response: " + resp, e);
        }
    }

    private String getInitializeTemplate(String targets, String input, String params) {
        return getTemplate("initialize", ImmutableMap.of( //
                "{{TARGETS}}", targets,
                "{{INPUT}}", input,
                "{{PARAMS}}", params
        ));
    }

    private String getLoadInputTemplate(String replace) {
        return getTemplate("load_input", ImmutableMap.of("{{INPUT}}", replace));
    }

    private String getLoadParamsTemplate(String replace) {
        return getTemplate("load_params", ImmutableMap.of("{{PARAMS}}", replace));
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
            result = IOUtils.toString(is, Charset.forName("UTF-8"));
        } catch (IOException e) {
            throw new RuntimeException("Failed to read " + resource, e);
        }
        for (Map.Entry<String, String> entry: replace.entrySet()) {
            String token = entry.getKey();
            String value = entry.getValue();
            if (SparkInterpreter.Scala.equals(interpreter)) {
                result = result.replace(token, StringEscapeUtils.escapeJava(value));
            } else {
                result = result.replace(token, value);
            }
        }
        return result;
    }

    private String parseOutput(String text) {
        boolean inOutputRegion = false;
        List<String> outputLines = new ArrayList<>();
        for (String line: StringUtils.split(text, "\n")) {
            if (inOutputRegion) {
                if (END_OUTPUT.equals(line)) {
                    inOutputRegion = false;
                } else {
                    outputLines.add(line);
                }
            } else {
                if (BEGIN_OUTPUT.equals(line)) {
                    inOutputRegion = true;
                }
            }
        }
        return StringUtils.join(outputLines, "\n");
    }

}
