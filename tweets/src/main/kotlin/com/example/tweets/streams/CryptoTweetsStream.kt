package com.example.tweets.streams

import com.example.tweets.avro.EntitySentiment
import com.example.tweets.clients.LanguageClient
import com.example.tweets.model.Tweet
import com.example.tweets.serdes.TweetSerdes
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.*
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration


@Configuration
class CryptoTweetsStream {


    @Bean
    fun tweetsStream(
        streamsBuilder: StreamsBuilder,
        entitySentimentSerde: SpecificAvroSerde<EntitySentiment>,
        languageClient: LanguageClient,
        tweetSerdes: TweetSerdes
    ): Topology {

        val stream: KStream<ByteArray, Tweet> =
            streamsBuilder.stream("tweets", Consumed.with(Serdes.ByteArray(), tweetSerdes))

        stream.print(Printed.toSysOut<ByteArray, Tweet>().withLabel("tweets-stream"))


        val filtered = stream.filterNot { _: ByteArray, tweet: Tweet -> tweet.retweet }

        val englishTweets = Predicate<ByteArray, Tweet> { _, tweet -> tweet.lang == "en" }

        val nonEnglishTweets = Predicate<ByteArray, Tweet> { _, tweet -> tweet.lang != "en" }

        val branches = filtered.branch(englishTweets, nonEnglishTweets)

        val englishStream = branches[0]
        englishStream.print(Printed.toSysOut<ByteArray, Tweet>().withLabel("tweets-english"))

        val nonEnglishStream = branches[1]
        nonEnglishStream.print(Printed.toSysOut<ByteArray, Tweet>().withLabel("tweets-non-english"))

        val translatedStream =
            nonEnglishStream.mapValues { tweet: Tweet -> languageClient.translate(tweet, "en") }

        val merged = englishStream.merge(translatedStream)

        val enriched =
            merged.flatMapValues { tweet: Tweet ->
                languageClient.getEntitySentiment(tweet).filter { currencies.contains(it.entity) }
            }


        enriched.to(
            "crypto-sentiment",
            Produced.with(Serdes.ByteArray(), entitySentimentSerde)
        )

        val topology = streamsBuilder.build()

        println(topology.describe())

        return topology

    }

    companion object {
        private val currencies = listOf("bitcoin", "ethereum")
    }

}