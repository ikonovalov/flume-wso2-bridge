package ru.codeunited.flume.wso2;

/**
 * Created by ikonovalov on 12/08/16.
 */
public final class EventConverterFactory {

    public static FlumeEventConverter create(String className) {
        try {
            return (FlumeEventConverter) Class.forName(className).newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
