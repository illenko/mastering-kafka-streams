package com.example.streams

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.Stores
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class ManagementTopology {
    @Bean
    fun topology(builder: StreamsBuilder): Topology {
        builder
            .stream<String, String>("patient-events")
            .groupByKey()
            .count(
                Materialized
                    .`as`<String, Long>(Stores.lruMap("counts", 10))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Long()),
            )

        return builder.build()
    }
}
