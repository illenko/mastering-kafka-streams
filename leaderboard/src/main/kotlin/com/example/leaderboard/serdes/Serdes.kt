package com.example.leaderboard.serdes

import com.example.leaderboard.domain.Player
import com.example.leaderboard.domain.Product
import com.example.leaderboard.domain.ScoreEvent
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration


@Configuration
class SerdesConfig {
    
    @Bean
    fun scoreEventSerde(objectMapper: ObjectMapper): Serde<ScoreEvent> = JsonSerdes(objectMapper)

    @Bean
    fun playerSerde(objectMapper: ObjectMapper): Serde<Player> = JsonSerdes(objectMapper)

    @Bean
    fun productSerde(objectMapper: ObjectMapper): Serde<Product> = JsonSerdes(objectMapper)
}

class JsonSerdes<T>(
    private val objectMapper: ObjectMapper
) : Serde<T> {

    override fun serializer(): Serializer<T> = Serializer { _, data ->
        objectMapper.writeValueAsBytes(data)
    }

    override fun deserializer(): Deserializer<T> = Deserializer { _, bytes ->
        objectMapper.readValue(bytes, object : com.fasterxml.jackson.core.type.TypeReference<T>() {})
    }
}