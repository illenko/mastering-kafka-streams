package com.example.monitoring.extractor

import com.example.monitoring.domain.Vital
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor
import org.springframework.stereotype.Component
import java.time.Instant

@Component
class VitalTimestampExtractor : TimestampExtractor {
    override fun extract(record: ConsumerRecord<Any, Any>, partitionTime: Long): Long {
        val measurement = record.value() as? Vital
        return measurement?.timestamp?.let { Instant.parse(it).toEpochMilli() } ?: partitionTime
    }
}