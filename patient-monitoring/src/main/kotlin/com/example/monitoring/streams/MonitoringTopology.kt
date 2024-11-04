package com.example.monitoring.streams

import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class MonitoringTopology {

    @Bean
    fun topology(
        builder: StreamsBuilder,
    ): Topology = builder.build()
}