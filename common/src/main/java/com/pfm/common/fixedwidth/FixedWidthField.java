package com.pfm.common.fixedwidth;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a record component as occupying a 1-indexed, inclusive-start position range
 * within a fixed-width line. {@code start} and {@code length} must match the field
 * position table in docs/file-spec.md.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.RECORD_COMPONENT)
public @interface FixedWidthField {
    int start();
    int length();
}
