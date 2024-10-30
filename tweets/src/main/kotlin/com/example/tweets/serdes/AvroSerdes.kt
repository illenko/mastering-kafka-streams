package com.example.tweets.serdes

import com.example.tweets.avro.EntitySentiment
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class AvroSerdes {

    @Bean
    fun kafkaSerdeConfig(
        @Value("\${spring.kafka.streams.properties.schema.registry.url}") schemaRegistryUrl: String
    ): Map<String, String> = mapOf("schema.registry.url" to schemaRegistryUrl)

    @Bean
    fun entitySentimentSerde(config: Map<String, String>): SpecificAvroSerde<EntitySentiment> =
        SpecificAvroSerde<EntitySentiment>().apply {
            configure(config, false)
        }
}