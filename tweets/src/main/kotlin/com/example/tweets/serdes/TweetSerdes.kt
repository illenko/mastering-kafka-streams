package com.example.tweets.serdes

import com.example.tweets.model.Tweet
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import org.springframework.stereotype.Component

@Component
class TweetSerdes(
    private val objectMapper: ObjectMapper
) : Serde<Tweet> {

    override fun serializer(): Serializer<Tweet> = Serializer { _, tweet ->
        objectMapper.writeValueAsBytes(tweet)
    }

    override fun deserializer(): Deserializer<Tweet> = Deserializer { _, bytes ->
        objectMapper.readValue(bytes, Tweet::class.java)
    }
}