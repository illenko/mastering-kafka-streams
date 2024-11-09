package com.example.management.config

import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.state.HostInfo
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration
import org.springframework.kafka.config.KafkaStreamsConfiguration
import org.springframework.kafka.support.serializer.JsonDeserializer

@Configuration
@EnableKafka
@EnableKafkaStreams
class KafkaStreamsConfig {
    @Bean(name = [KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME])
    fun kStreamsConfigs(
        kafkaProperties: KafkaProperties,
        @Value("\${spring.kafka.streams.application-id}") appName: String,
        @Value("\${spring.kafka.streams.properties.state.dir}") stateDir: String,
        @Value("\${server.host}") host: String,
        @Value("\${server.port}") port: String,
    ): KafkaStreamsConfiguration =
        KafkaStreamsConfiguration(
            mapOf(
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaProperties.bootstrapServers,
                StreamsConfig.APPLICATION_ID_CONFIG to appName,
                StreamsConfig.STATE_DIR_CONFIG to stateDir,
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String()::class.java,
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to Serdes.String()::class.java,
                StreamsConfig.APPLICATION_SERVER_CONFIG to "$host:$port",
                StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG to WallclockTimestampExtractor::class.java,
                StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG to LogAndContinueExceptionHandler::class.java,
                JsonDeserializer.VALUE_DEFAULT_TYPE to JsonNode::class.java,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ),
        )

    @Bean
    fun hostInfo(
        @Value("\${server.host}") host: String,
        @Value("\${server.port}") port: String,
    ): HostInfo = HostInfo(host, port.toInt())
}
