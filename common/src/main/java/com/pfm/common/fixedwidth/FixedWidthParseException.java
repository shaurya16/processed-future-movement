package com.pfm.common.fixedwidth;

public class FixedWidthParseException extends RuntimeException {

    private final int lineNumber;
    private final String rawLine;

    public FixedWidthParseException(int lineNumber, String rawLine, String reason) {
        super("Line " + lineNumber + ": " + reason);
        this.lineNumber = lineNumber;
        this.rawLine = rawLine;
    }

    public int lineNumber() {
        return lineNumber;
    }

    public String rawLine() {
        return rawLine;
    }
}
