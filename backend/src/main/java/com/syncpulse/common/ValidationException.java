package com.syncpulse.common;

/** Thrown for semantic validation failures that map to HTTP 400. */
public class ValidationException extends RuntimeException {
    public ValidationException(String message) {
        super(message);
    }
}
