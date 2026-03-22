package com.shekhar.consumerkafkaa.exception;

public class RetryableException extends RuntimeException {
    public RetryableException(String message, Throwable cause) {
        super(message, cause);
    }

}
