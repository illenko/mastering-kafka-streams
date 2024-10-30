package com.example.tweets.clients

import com.example.tweets.avro.EntitySentiment
import com.example.tweets.model.Tweet
import org.springframework.stereotype.Component
import kotlin.random.Random

@Component
class LanguageClient {

    fun translate(tweet: Tweet, targetLanguage: String): Tweet {
        return tweet.copy(text = "Translated: ${tweet.text}")
    }

    fun getEntitySentiment(tweet: Tweet): List<EntitySentiment> =
        tweet.text
            .toLowerCase()
            .replace("#", "")
            .split(" ")
            .map {
                EntitySentiment.newBuilder()
                    .setCreatedAt(tweet.createdAt)
                    .setId(tweet.id)
                    .setEntity(it)
                    .setText(tweet.text)
                    .setSalience(randomDouble())
                    .setSentimentScore(randomDouble())
                    .setSentimentMagnitude(randomDouble())
                    .build()
            }


    private fun randomDouble(): Double = Random.nextDouble(0.0, 1.0)

}