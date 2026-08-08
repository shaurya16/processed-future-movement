package com.pfm.common.fixedwidth;

public class FixedWidthParseException extends RuntimeException {

    private final int lineNumber;
    private final String rawLine;
    private final String reason;

    public FixedWidthParseException(int lineNumber, String rawLine, String reason) {
        super("Line " + lineNumber + ": " + reason);
        this.lineNumber = lineNumber;
        this.rawLine = rawLine;
        this.reason = reason;
    }

    public int lineNumber() {
        return lineNumber;
    }

    public String rawLine() {
        return rawLine;
    }

    /** The original failure reason, without the "Line N: " prefix that {@link #getMessage()} carries. */
    public String reason() {
        return reason;
    }
}
