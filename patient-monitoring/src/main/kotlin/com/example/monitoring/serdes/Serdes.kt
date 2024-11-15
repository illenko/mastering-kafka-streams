package com.example.monitoring.serdes

import com.example.monitoring.domain.BodyTemp
import com.example.monitoring.domain.CombinedVitals
import com.example.monitoring.domain.Pulse
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class SerdesConfig {

    @Bean
    fun pulseSerde(mapper: ObjectMapper): Serde<Pulse> = serde(mapper)

    @Bean
    fun bodyTempSerde(mapper: ObjectMapper): Serde<BodyTemp> = serde(mapper)

    @Bean
    fun combinedVitalsSerde(mapper: ObjectMapper): Serde<CombinedVitals> = serde(mapper)

    private inline fun <reified T> serde(mapper: ObjectMapper): Serde<T> = Serdes.serdeFrom(
        { _, data -> mapper.writeValueAsBytes(data) },
        { _, bytes -> mapper.readValue(bytes, T::class.java) }
    )
}