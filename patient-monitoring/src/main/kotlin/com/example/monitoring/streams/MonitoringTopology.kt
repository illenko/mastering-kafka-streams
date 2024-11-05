package com.example.monitoring.streams

import com.example.monitoring.domain.BodyTemp
import com.example.monitoring.domain.CombinedVitals
import com.example.monitoring.domain.Pulse
import com.example.monitoring.extractor.VitalTimestampExtractor
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

@Configuration
class MonitoringTopology {

    @Bean
    fun topology(
        builder: StreamsBuilder,
        pulseSerde: Serde<Pulse>,
        bodyTempSerde: Serde<BodyTemp>,
        combinedVitalsSerde: Serde<CombinedVitals>,
        vitalTimestampExtractor: VitalTimestampExtractor,
    ): Topology {

        val pulseEvents = builder.stream(
            "pulse-events", Consumed.with(Serdes.String(), pulseSerde)
                .withTimestampExtractor(vitalTimestampExtractor)
        )

        val tempEvents =
            builder.stream(
                "body-temp-events", Consumed.with(Serdes.String(), bodyTempSerde)
                    .withTimestampExtractor(vitalTimestampExtractor)
            )

        val tumblingWindow =
            TimeWindows.of(Duration.ofSeconds(60)).grace(Duration.ofSeconds(5))

        val pulseCounts =
            pulseEvents
                .groupByKey()
                .windowedBy(tumblingWindow)
                .count(Materialized.`as`("pulse-counts"))
                .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded().shutDownWhenFull()))

        val highPulse: KStream<String, Long> =
            pulseCounts
                .toStream()
                .peek { key, value ->
                    val id = key.key()
                    val start = key.window().start()
                    val end = key.window().end()
                    println("Patient $id had a heart rate of $value between $start and $end")
                }
                .filter { _, value -> value >= 100 }
                .map { windowedKey, value -> KeyValue.pair(windowedKey.key(), value) }

        val highTemp =
            tempEvents.filter { _, value -> value.temperature > 100.4 }

        val joinParams =
            StreamJoined.with(Serdes.String(), Serdes.Long(), bodyTempSerde)

        val joinWindows =
            JoinWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(60), Duration.ofSeconds(10))

        val valueJoiner =
            ValueJoiner { pulseRate: Long, bodyTemp: BodyTemp ->
                CombinedVitals(
                    heartRate = pulseRate.toInt(),
                    bodyTemp = bodyTemp,
                )
            }

        val vitalsJoined =
            highPulse.join(highTemp, valueJoiner, joinWindows, joinParams)


        vitalsJoined.to("alerts", Produced.with(Serdes.String(), combinedVitalsSerde))

        // debug only
        pulseCounts
            .toStream()
            .print(Printed.toSysOut<Windowed<String>, Long>().withLabel("pulse-counts"))
        highPulse.print(Printed.toSysOut<String, Long>().withLabel("high-pulse"))
        highTemp.print(Printed.toSysOut<String, BodyTemp>().withLabel("high-temp"))
        vitalsJoined.print(Printed.toSysOut<String, CombinedVitals>().withLabel("vitals-joined"))

        return builder.build()
    }
}