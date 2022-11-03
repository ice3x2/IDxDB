package com.snoworca.IdxDB;

class IdxDBLogger {

    private static LoggerDelegator loggerDelegator = null;

    public static boolean isDebug() {
        if(loggerDelegator == null) return false;
        return loggerDelegator.isDebug();
    }

    public static boolean isInfo() {
        if(loggerDelegator == null) return false;
        return loggerDelegator.isInfo();
    }

    public static void setLoggerDelegator(LoggerDelegator loggerDelegator) {
        IdxDBLogger.loggerDelegator = loggerDelegator;
    }

    public static void debug(String message) {
        if(loggerDelegator != null) {
            loggerDelegator.debug(message);
        }
    }

    public static void info(String message) {
        if(loggerDelegator != null) {
            loggerDelegator.info(message);
        }
    }

    public static void warn(String message) {
        if(loggerDelegator != null) {
            loggerDelegator.warn(message);
        }
    }

    public static void warn(String message, Throwable t) {
        if(loggerDelegator != null) {
            loggerDelegator.warn(message, t);
        }
    }

    public static void error(String message) {
        if(loggerDelegator != null) {
            loggerDelegator.error(message);
        }
    }

    public static void error(String message, Throwable t) {
        if(loggerDelegator != null) {
            loggerDelegator.error(message, t);
        }
    }

    public static void fatal(String message) {
        if(loggerDelegator != null) {
            loggerDelegator.fatal(message);
        }
    }

    public static void fatal(String message, Throwable t) {
        if(loggerDelegator != null) {
            loggerDelegator.fatal(message, t);
        }
    }











}
