package com.reparatio;

/** Base exception for all Reparatio API errors. */
public class ReparatioException extends RuntimeException {
    private final int statusCode;

    public ReparatioException(int statusCode, String message) {
        super(message);
        this.statusCode = statusCode;
    }

    public int getStatusCode() { return statusCode; }

    public static class AuthenticationException extends ReparatioException {
        public AuthenticationException(int status, String message) { super(status, message); }
    }

    public static class InsufficientPlanException extends ReparatioException {
        public InsufficientPlanException(String message) { super(402, message); }
    }

    public static class FileTooLargeException extends ReparatioException {
        public FileTooLargeException(String message) { super(413, message); }
    }

    public static class ParseException extends ReparatioException {
        public ParseException(String message) { super(422, message); }
    }
}
