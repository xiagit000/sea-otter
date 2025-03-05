package com.ybt.seaotter.exceptions;

import java.io.IOException;

public class SeaOtterException extends RuntimeException {

    public SeaOtterException(String message) {
        super(message);
    }

    public SeaOtterException(String message, Throwable cause) {
        super(message, cause);
    }

    public SeaOtterException(IOException e) {
        super(e);
    }
}
