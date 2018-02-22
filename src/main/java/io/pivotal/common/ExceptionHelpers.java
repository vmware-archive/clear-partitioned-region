package io.pivotal.common;

import com.gemstone.gemfire.LogWriter;

public class ExceptionHelpers {
    public static String logException(Exception e, LogWriter log) {
        RuntimeException serializableException = new RuntimeException(e.getMessage());
        serializableException.setStackTrace(e.getStackTrace());
        log.error(e);
        return e.getMessage();
    }
}
