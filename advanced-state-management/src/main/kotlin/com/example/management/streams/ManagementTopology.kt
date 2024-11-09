package com.example.management.streams

import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class ManagementTopology {

    @Bean
    fun topology(
        builder: StreamsBuilder,
    ): Topology {
        return builder.build()
    }
}