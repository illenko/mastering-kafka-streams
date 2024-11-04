package com.example.leaderboard.config

import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
import org.apache.kafka.streams.state.HostInfo
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration
import org.springframework.kafka.config.KafkaStreamsConfiguration
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer
import org.springframework.kafka.support.serializer.JsonDeserializer
import java.util.*

@Configuration
class KafkaStreamsConfig {
    @Value("\${spring.kafka.streams.application-id}")
    private lateinit var appName: String

    @Value("\${spring.kafka.streams.properties.default.key.serde}")
    private lateinit var defaultKeySerde: String

    @Value("\${spring.kafka.streams.properties.default.value.serde}")
    private lateinit var defaultValueSerde: String

    @Value("\${spring.kafka.streams.properties.state.dir}")
    private lateinit var stateDir: String

    @Bean
    fun streamsBuilderFactoryBeanConfigurer(): StreamsBuilderFactoryBeanConfigurer =
        StreamsBuilderFactoryBeanConfigurer { factoryBean ->
            factoryBean.setKafkaStreamsCustomizer { kafkaStreams ->
                kafkaStreams.setStateListener { newState, oldState ->
                    if (newState == KafkaStreams.State.ERROR) {
                        println("Error state transition from $oldState to $newState")
                    }
                }
            }
        }

    @Bean
    fun streamsBuilderFactoryBean(kStreamsConfigs: KafkaStreamsConfiguration): StreamsBuilderFactoryBean =
        StreamsBuilderFactoryBean(kStreamsConfigs).apply {
            isAutoStartup = true
        }

    @Bean(name = [KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME])
    fun kStreamsConfigs(
        @Value("\${spring.kafka.streams.application-id}") applicationId: String,
        @Value("\${spring.kafka.bootstrap-servers}") bootstrapServers: String,
    ): KafkaStreamsConfiguration =
        KafkaStreamsConfiguration(
            mapOf(
                StreamsConfig.APPLICATION_ID_CONFIG to applicationId,
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ),
        )

    @Bean
    fun kafkaStreams(
        kafkaProperties: KafkaProperties,
        topology: Topology,
        @Value("\${server.port}") port: String,
        @Value("\${server.host}") host: String,
    ): KafkaStreams {
        val props =
            Properties().apply {
                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.bootstrapServers)
                put(StreamsConfig.APPLICATION_ID_CONFIG, appName)
                put(StreamsConfig.STATE_DIR_CONFIG, stateDir)
                put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, defaultKeySerde)
                put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, defaultValueSerde)
                put(StreamsConfig.APPLICATION_SERVER_CONFIG, "$host:$port")
                put(
                    StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                    LogAndContinueExceptionHandler::class.java
                )
                put(JsonDeserializer.VALUE_DEFAULT_TYPE, JsonNode::class.java)
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            }

        return KafkaStreams(topology, props).apply {
            start()
        }
    }

    @Bean
    fun hostInfo(
        @Value("\${server.host}") host: String,
        @Value("\${server.port}") port: String,
    ): HostInfo = HostInfo(host, port.toInt())
}