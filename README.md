# flume-wso2-bridge
Apache Flume sink component with WSO2 Carbon databridge events as a target. 
<pre>
# AVRO(5141) -> Memory channel -> WSO2 Carbon (TCP)

agent2.sources = avroSrc
agent2.channels = memoryChannel
agent2.sinks = wso2carbonSink

# SOURCE
agent2.sources.avroSrc.type = avro
agent2.sources.avroSrc.bind = 0.0.0.0
agent2.sources.avroSrc.port = 5141
agent2.sources.avroSrc.channels = memoryChannel
agent2.sources.avroSrc.interceptors = timestamp_interceptor
agent2.sources.avroSrc.interceptors.timestamp_interceptor.type = timestamp


# CHANNEL
agent2.channels.memoryChannel.type = memory
agent2.channels.memoryChannel.capacity = 100

# SINK
agent2.sinks.wso2carbonSink.type = ru.smsoft.cep.WSO2CarbonSink
agent2.sinks.wso2carbonSink.channel = memoryChannel
agent2.sinks.wso2carbonSink.carbon_agent_config = plugins.d/wso2-carbon-cep/data-agent-conf.xml
</pre>