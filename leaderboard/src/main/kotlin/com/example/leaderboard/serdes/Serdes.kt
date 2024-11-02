package com.example.leaderboard.serdes

import com.example.leaderboard.domain.Enriched
import com.example.leaderboard.domain.Player
import com.example.leaderboard.domain.Product
import com.example.leaderboard.domain.ScoreEvent
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class SerdesConfig {

    @Bean
    fun scoreEventSerde(objectMapper: ObjectMapper): Serde<ScoreEvent> = createSerde(objectMapper)

    @Bean
    fun playerSerde(objectMapper: ObjectMapper): Serde<Player> = createSerde(objectMapper)

    @Bean
    fun productSerde(objectMapper: ObjectMapper): Serde<Product> = createSerde(objectMapper)

    @Bean
    fun enrichedSerde(objectMapper: ObjectMapper): Serde<Enriched> = createSerde(objectMapper)

    private inline fun <reified T> createSerde(objectMapper: ObjectMapper): Serde<T> = Serdes.serdeFrom(
        { _, data -> objectMapper.writeValueAsBytes(data) },
        { _, bytes -> objectMapper.readValue(bytes, T::class.java) }
    )
}