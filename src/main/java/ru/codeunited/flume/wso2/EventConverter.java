package ru.codeunited.flume.wso2;



/**
 * Created by ikonovalov on 23/03/16.
 */
public interface EventConverter<T> {

    org.wso2.carbon.databridge.commons.Event convert(T event);

}
