package com.example.processors

import com.example.model.DigitalTwin
import com.example.model.State
import com.example.model.Type
import org.apache.kafka.streams.processor.Cancellable
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant

@Component
class DigitalTwinProcessor : Processor<String, State, String, DigitalTwin> {
    private lateinit var context: ProcessorContext<String, DigitalTwin>
    private lateinit var kvStore: KeyValueStore<String, DigitalTwin>
    private lateinit var punctuator: Cancellable

    override fun init(context: ProcessorContext<String, DigitalTwin>) {
        this.context = context
        this.kvStore = context.getStateStore("digital-twin-store") as KeyValueStore<String, DigitalTwin>

        punctuator =
            this.context.schedule(
                Duration.ofMinutes(5),
                PunctuationType.WALL_CLOCK_TIME,
                this::enforceTtl,
            )

        this.context.schedule(
            Duration.ofSeconds(20),
            PunctuationType.WALL_CLOCK_TIME,
            { context.commit() },
        )
    }

    override fun process(record: Record<String, State>) {
        val key = record.key()
        val value = record.value()
        println("Processing: $value")

        val digitalTwin =
            (kvStore[key] ?: DigitalTwin()).apply {
                when (value.type) {
                    Type.DESIRED -> desired = value
                    Type.REPORTED -> reported = value
                }
            }

        println("Storing digital twin: $digitalTwin}")
        kvStore.put(key, digitalTwin)

        val newRecord = Record(record.key(), digitalTwin, record.timestamp())
        context.forward(newRecord)
    }

    override fun close() {
        punctuator.cancel()
    }

    private fun enforceTtl(timestamp: Long) {
        kvStore.all().use {
            while (it.hasNext()) {
                val entry = it.next()
                println("Checking to see if digital twin record has expired: $entry.key")
                val lastReportedState = entry.value.reported ?: continue

                val lastUpdated = Instant.parse(lastReportedState.timestamp)
                val daysSinceLastUpdate = Duration.between(lastUpdated, Instant.now()).toDays()
                if (daysSinceLastUpdate >= 7) {
                    kvStore.delete(entry.key)
                }
            }
        }
    }
}
