package com.example.streams

import com.example.model.DigitalTwin
import com.example.model.State
import com.example.processors.DigitalTwinProcessor
import com.example.processors.HighWindsFlatmapProcessor
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.state.Stores
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class DigitalTwinTopology {
    @Bean
    fun topology(
        stateSerde: Serde<State>,
        digitalTwinSerde: Serde<DigitalTwin>,
    ): Topology {
        val builder = Topology()

        builder.addSource(
            "Desired State Events",
            Serdes.String().deserializer(),
            stateSerde.deserializer(),
            "desired-state-events",
        )

        builder.addSource(
            "Reported State Events",
            Serdes.String().deserializer(),
            stateSerde.deserializer(),
            "reported-state-events",
        )

        builder.addProcessor<String, State, String, State>(
            "High Winds Flatmap Processor",
            { HighWindsFlatmapProcessor() },
            "Reported State Events",
        )

        builder.addProcessor<String, State, String, DigitalTwin>(
            "Digital Twin Processor",
            { DigitalTwinProcessor() },
            "High Winds Flatmap Processor",
            "Desired State Events",
        )

        val storeBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("digital-twin-store"),
                Serdes.String(),
                digitalTwinSerde,
            )

        builder.addStateStore(
            storeBuilder,
            "Digital Twin Processor",
        )

        builder.addSink(
            "Digital Twin Sink",
            "digital-twins",
            Serdes.String().serializer(),
            digitalTwinSerde.serializer(),
            "Digital Twin Processor",
        )

        return builder
    }
}
