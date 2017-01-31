package com.latticeengines.domain.exposed.camille.locks;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RateLimitingStatus {

    @JsonProperty("History")
    private Map<String, Map<Long, Long>> history = new HashMap<>();

    public Map<String, Map<Long, Long>> getHistory() {
        return history;
    }

    public void setHistory(Map<String, Map<Long, Long>> history) {
        this.history = history;
    }

    private Long sumCounter(String counter) {
        if (history.containsKey(counter)) {
            return history.get(counter).entrySet().stream().reduce(0L, (acc, entry) -> acc + entry.getValue(),
                    (acc1, acc2) -> acc1 + acc2);
        } else {
            return 0L;
        }
    }
}
