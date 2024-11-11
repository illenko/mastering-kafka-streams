package com.example.serdes

import com.example.model.DigitalTwin
import com.example.model.State
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class SerdesConfig {
    @Bean
    fun digitalTwinSerde(mapper: ObjectMapper): Serde<DigitalTwin> = serde(mapper)

    @Bean
    fun stateSerde(mapper: ObjectMapper): Serde<State> = serde(mapper)

    private inline fun <reified T> serde(mapper: ObjectMapper): Serde<T> =
        Serdes.serdeFrom(
            { _, data -> mapper.writeValueAsBytes(data) },
            { _, bytes -> mapper.readValue(bytes, T::class.java) },
        )
}
