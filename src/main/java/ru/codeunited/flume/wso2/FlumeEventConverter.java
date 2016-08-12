package ru.codeunited.flume.wso2;

import org.wso2.carbon.databridge.commons.Event;

/**
 * Created by ikonovalov on 23/03/16.
 */
public class FlumeEventConverter implements EventConverter<org.apache.flume.Event> {

    @Override
    public Event convert(org.apache.flume.Event flumeEvent) {
        Event carbonEvent = new Event();
        carbonEvent.setPayloadData(new Object[] {
                flumeEvent.getBody()
        });
        return carbonEvent;
    }
}
