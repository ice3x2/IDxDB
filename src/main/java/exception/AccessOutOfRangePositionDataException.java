package exception;

public class AccessOutOfRangePositionDataException extends RuntimeException{
    public AccessOutOfRangePositionDataException(long len, long pos) {
        super("Attempt to access out of range position data. (data length: " + len + ", pos:" + pos + ")");
    }
}
