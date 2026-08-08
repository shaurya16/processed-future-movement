package com.pfm.ingestion;

import com.pfm.common.fixedwidth.ParseError;

import java.util.List;

public class KafkaPublishException extends RuntimeException {

    private final List<ParseError> failures;

    public KafkaPublishException(String message, List<ParseError> failures) {
        super(message);
        this.failures = failures;
    }

    public List<ParseError> failures() {
        return failures;
    }
}
