package com.pfm.common.domain;

import com.pfm.common.fixedwidth.FixedWidthParseException;
import com.pfm.common.fixedwidth.FixedWidthRecordParser;
import com.pfm.common.fixedwidth.ParseError;

import java.util.ArrayList;
import java.util.List;

/**
 * Domain-specific facade composing {@link FixedWidthRecordParser} and
 * {@link FutureTransactionFactory}. {@link #parse} is strict (throws on a bad line);
 * {@link #parseAll} is resilient (skip-and-collect), which is what a file-reading
 * caller like ingestion-service is expected to use.
 */
public class FutureTransactionParser {

    private final FixedWidthRecordParser recordParser = new FixedWidthRecordParser();
    private final FutureTransactionFactory factory = new FutureTransactionFactory();

    public FutureTransaction parse(String line, int lineNumber) {
        RawFutureTransaction raw = recordParser.parse(line, lineNumber, RawFutureTransaction.class);
        return factory.from(raw, lineNumber, line);
    }

    public ParseResult parseAll(List<String> lines) {
        List<ParsedRecord> records = new ArrayList<>();
        List<ParseError> errors = new ArrayList<>();

        for (int i = 0; i < lines.size(); i++) {
            int lineNumber = i + 1;
            try {
                records.add(new ParsedRecord(lineNumber, parse(lines.get(i), lineNumber)));
            } catch (FixedWidthParseException e) {
                errors.add(new ParseError(e.lineNumber(), e.rawLine(), e.reason()));
            }
        }

        return new ParseResult(records, errors);
    }
}
