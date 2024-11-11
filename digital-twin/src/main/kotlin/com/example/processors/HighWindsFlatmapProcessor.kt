package com.example.processors

import com.example.model.Power
import com.example.model.State
import com.example.model.Type
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.springframework.stereotype.Component

@Component
class HighWindsFlatmapProcessor : Processor<String, State, String, State> {
    private lateinit var context: ProcessorContext<String, State>

    override fun init(context: ProcessorContext<String, State>) {
        this.context = context
    }

    override fun process(record: Record<String, State>) {
        val reported = record.value()
        context.forward(record)

        if (reported.windSpeedMph > 65 && reported.power == Power.ON) {
            val desired = reported.copy(power = Power.OFF, type = Type.DESIRED)
            val newRecord = Record(record.key(), desired, record.timestamp())
            context.forward(newRecord)
        }
    }

    override fun close() {
        // nothing to do
    }
}
