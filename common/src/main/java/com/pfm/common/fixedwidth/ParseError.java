package com.pfm.common.fixedwidth;

/** One line that failed to parse, with enough context to build an error report. */
public record ParseError(int lineNumber, String rawLine, String reason) {
}
