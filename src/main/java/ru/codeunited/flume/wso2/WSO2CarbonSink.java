package ru.codeunited.flume.wso2;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.databridge.agent.*;
import org.wso2.carbon.databridge.agent.exception.*;
import org.wso2.carbon.databridge.commons.exception.TransportException;

/**
 * Created by ikonovalov on 30/03/16.
 */
public class WSO2CarbonSink extends AbstractSink implements Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(WSO2CarbonSink.class);
    public static final long TIMEOUT_MS = 50L;

    private DataPublisher dataPublisher;

    private FlumeEventConverter carbonConverter;

    private String carbonStreamId;

    private static final Protocol AGENT_PROTOCOL = Protocol.Thrift;
    private static final String THRIFT_TCP = "thrift_tcp";
    private static final String THRIFT_SSL = "thrift_ssl";
    private static final String STREAM_DEFINITION = "carbon_stream";
    private static final String CARBON_USER = "carbon_user";
    private static final String CARBON_PASSWORD = "carbon_password";
    private static final String CARBON_AGENT_CONFIG_LOCATION = "carbon_agent_config";

    @Override
    public synchronized void stop() {
        if (dataPublisher != null) {
            try {
                dataPublisher.shutdownWithAgent();
                LOG.info("Shutdown WSO2 Carbon data publisher with an agent");
            } catch (DataEndpointException e) {
                LOG.error("Shutdown WSO2 Carbon data publisher with error.", e);
            }
        }
        super.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.BACKOFF;

        // Start transaction
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();

        try {
            txn.begin();
            // This try clause includes whatever Channel operations you want to do

            Event event = ch.take();
            if (event != null) {
                org.wso2.carbon.databridge.commons.Event carbonEvent = carbonConverter.convert(event);
                carbonEvent.setStreamId(carbonStreamId);
                carbonEvent.setTimeStamp(System.currentTimeMillis());

                boolean publishAccepted = dataPublisher.tryPublish(carbonEvent, TIMEOUT_MS);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Event published {}", publishAccepted);
                }
                status = Status.READY;
            }
            txn.commit();

        } catch (Exception t) {
            txn.rollback();
            LOG.error(t.getMessage(), t);
            throw new EventDeliveryException(t.getMessage(), t);

        } finally {
            txn.close();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Polling -> {}", status.name());
        }
        return status;
    }

    @Override
    public void configure(Context context) {
        String agentConfig = context.getString(CARBON_AGENT_CONFIG_LOCATION, "data-agent-conf.xml");
        AgentHolder.setConfigPath(agentConfig);
        String thriftTCP = context.getString(THRIFT_TCP, "tcp://localhost:7611");
        String thriftSSL = context.getString(THRIFT_SSL, "ssl://localhost:7711");
        String carbonStream = context.getString(STREAM_DEFINITION, "ru.smsoft.hitcounter.flume:1.0.0");
        String carbonUser = context.getString(CARBON_USER, "admin");
        String carbonPassword = context.getString(CARBON_PASSWORD, "admin");

        String eventConverterClass = context.getString("event_converter_class");

        if (LOG.isInfoEnabled()) {
            LOG.info("WSO2 Carbon data agent config: {}", agentConfig);
            LOG.info("WSO2 Carbon thrift TCP: {}", thriftTCP);
            LOG.info("WSO2 Carbon thrift SSL: {}", thriftSSL);
            LOG.info("WSO2 Carbon streamL: {}", carbonStream);
            LOG.info("WSO2 Carbon username: {}", carbonUser);
            LOG.info("WSO2 Carbon password: {}", carbonPassword.replaceAll(".", "*"));
        }

        try {
            if (dataPublisher != null) { // if a reconfiguration occurred.
                dataPublisher.shutdown();
                LOG.info("Shutdown WSO2 Carbon data publisher and waiting for a new instance...");
            }

            dataPublisher = new DataPublisher(
                    AGENT_PROTOCOL.name(),
                    thriftTCP, thriftSSL,
                    carbonUser, carbonPassword
            );

            LOG.info("WSO2 Carbon DataPublisher initialized");

            carbonConverter = EventConverterFactory.create(eventConverterClass);
            LOG.info("Using {} event converted", carbonConverter.getClass().getName());

            carbonStreamId = carbonStream;

        } catch (DataEndpointAgentConfigurationException e) {
            LOG.error("Agent configuration issue", e);
        } catch (DataEndpointException | DataEndpointConfigurationException | DataEndpointAuthenticationException e) {
            LOG.error("DataEndpoint issue", e);
        } catch (TransportException e) {
            LOG.error("Transport issue", e);
        }
    }
}
