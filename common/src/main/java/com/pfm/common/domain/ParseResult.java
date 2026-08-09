package com.pfm.common.domain;

import com.pfm.common.fixedwidth.ParseError;

import java.util.List;

public record ParseResult(List<ParsedRecord> records, List<ParseError> errors) {
}
