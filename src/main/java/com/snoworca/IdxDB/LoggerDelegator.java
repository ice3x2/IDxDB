package com.snoworca.IdxDB;

public interface LoggerDelegator {
        public void info(String message);
        public void error(String message);
        public void error(String message, Throwable t);
        public void debug(String message);
        public default void fatal(String message) {
                error(message);
        }
        public default  void fatal(String message, Throwable t) {
                error(message,t);
        }

        public void debug(String message, Throwable t);
        public void warn(String message);
        public void warn(String message, Throwable t);

        public default boolean isDebug() {
                return true;
        }
        public default boolean isInfo() {
                return true;
        }
        public default boolean isWarn() {
                return true;
        }


}
