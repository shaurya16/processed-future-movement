package com.pfm.common.fixedwidth;

import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;

/**
 * Generic parser that builds an all-String record type from a fixed-width line, using
 * each record component's {@link FixedWidthField} annotation to locate its substring.
 * Reusable for any record type meeting that shape, not just future-transaction records.
 */
public class FixedWidthRecordParser {

    public <T> T parse(String line, int lineNumber, Class<T> type) {
        RecordComponent[] components = type.getRecordComponents();
        Object[] args = new Object[components.length];
        Class<?>[] paramTypes = new Class<?>[components.length];

        for (int i = 0; i < components.length; i++) {
            RecordComponent component = components[i];
            FixedWidthField field = component.getAnnotation(FixedWidthField.class);
            if (field == null) {
                throw new FixedWidthParseException(lineNumber, line,
                        "Record component '" + component.getName() + "' has no @FixedWidthField");
            }

            int startIndex = field.start() - 1;
            int endIndex = startIndex + field.length();
            if (endIndex > line.length()) {
                throw new FixedWidthParseException(lineNumber, line,
                        "Field '" + component.getName() + "' needs " + endIndex
                                + " characters but line has " + line.length());
            }

            args[i] = line.substring(startIndex, endIndex).trim();
            paramTypes[i] = component.getType();
        }

        try {
            Constructor<T> constructor = type.getDeclaredConstructor(paramTypes);
            return constructor.newInstance(args);
        } catch (ReflectiveOperationException e) {
            throw new FixedWidthParseException(lineNumber, line,
                    "Failed to construct " + type.getSimpleName() + ": " + e.getMessage());
        }
    }
}
