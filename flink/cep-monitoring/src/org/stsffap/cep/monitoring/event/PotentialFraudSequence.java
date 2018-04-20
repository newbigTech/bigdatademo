package org.stsffap.cep.monitoring.event;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

public class PotentialFraudSequence {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @JsonProperty
    private final List<LoginEvent> failedAttempts;
    @JsonProperty
    private final List<LoginEvent> successfullAttempt;

    public PotentialFraudSequence(
            final List<LoginEvent> failedAttempts,
            final List<LoginEvent> successfullAttempt) {
        this.failedAttempts = failedAttempts;
        this.successfullAttempt = successfullAttempt;
    }

    @Override
    public String toString() {
        String toReturn = "";
        try {
            toReturn = MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return toReturn;
    }

}
