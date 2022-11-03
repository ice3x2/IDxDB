package com.snoworca.IdxDB;

public interface LoggerDelegator {
        public void info(String message);
        public void error(String message);
        public void error(String message, Throwable t);
        public void debug(String message);
        public void fatal(String message);
        public void fatal(String message, Throwable t);

        public void debug(String message, Throwable t);
        public void warn(String message);
        public void warn(String message, Throwable t);

        public boolean isDebug();
        public boolean isInfo();
        public boolean isWarn();


}
