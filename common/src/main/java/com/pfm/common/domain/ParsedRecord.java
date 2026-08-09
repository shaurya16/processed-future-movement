package com.pfm.common.domain;

/** A successfully-parsed record paired with the 1-indexed source line it came from. */
public record ParsedRecord(int lineNumber, FutureTransaction transaction) {
}
