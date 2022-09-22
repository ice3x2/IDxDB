package exception;

public class UnserializableTypeException extends RuntimeException {

    public UnserializableTypeException(Class<?> type) {
        super("'" + type.getName() + "' class must use the '@Serializable' annotation to enable serialization.");
    }



}
