package com.example.leaderboard.serdes

import com.example.leaderboard.domain.*
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class SerdesConfig {

    @Bean
    fun scoreEventSerde(mapper: ObjectMapper): Serde<ScoreEvent> = serde(mapper)

    @Bean
    fun playerSerde(mapper: ObjectMapper): Serde<Player> = serde(mapper)

    @Bean
    fun productSerde(mapper: ObjectMapper): Serde<Product> = serde(mapper)

    @Bean
    fun enrichedSerde(mapper: ObjectMapper): Serde<Enriched> = serde(mapper)

    @Bean
    fun highScoresSerde(mapper: ObjectMapper): Serde<HighScores> = serde(mapper)

    private inline fun <reified T> serde(mapper: ObjectMapper): Serde<T> = Serdes.serdeFrom(
        { _, data -> mapper.writeValueAsBytes(data) },
        { _, bytes -> mapper.readValue(bytes, T::class.java) }
    )
}