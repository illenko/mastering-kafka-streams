package com.example.leaderboard.streams

import com.example.leaderboard.domain.*
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.Joined
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.KeyValueStore
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration


@Configuration
class LeaderboardStream {

    @Bean
    fun playerScoreStream(
        builder: StreamsBuilder,
        scoreEventSerde: Serde<ScoreEvent>,
        playerSerde: Serde<Player>,
        productSerde: Serde<Product>,
        enrichedSerde: Serde<Enriched>,
        highScoresSerde: Serde<HighScores>,
    ): Topology {

        val scoreEvents = builder.stream("score-events", Consumed.with(Serdes.String(), scoreEventSerde))
            .selectKey { _, scoreEvent -> scoreEvent.playerId.toString() }

        val players = builder.table("players", Consumed.with(Serdes.String(), playerSerde))

        val products = builder.globalTable("products", Consumed.with(Serdes.String(), productSerde))

        val eventsWithPlayers = scoreEvents.join(
            players,
            { s, p -> ScoreWithPlayer(s, p) },
            Joined.with(Serdes.String(), scoreEventSerde, playerSerde)
        )

        val enrichedEvents = eventsWithPlayers.join(products,
            { _: String?, sp: ScoreWithPlayer -> sp.scoreEvent.productId.toString() },
            { s, p -> Enriched(s, p) }
        )

        val groupedEnrichedEvents =
            enrichedEvents.groupBy(
                { _: String, value: Enriched -> value.productId.toString() },
                Grouped.with(Serdes.String(), enrichedSerde)
            )

        val groupedPlayers =
            players.groupBy(
                { key: String, value: Player -> KeyValue.pair(key, value) },
                Grouped.with(Serdes.String(), playerSerde)
            )

        val highScores = groupedEnrichedEvents.aggregate(
            { HighScores() },
            { _: String?, value: Enriched, aggregate: HighScores -> aggregate.add(value) },
            Materialized.`as`<String, HighScores, KeyValueStore<Bytes, ByteArray>>("leader-boards")
                .withKeySerde(Serdes.String()).withValueSerde(highScoresSerde)
        )

        highScores.toStream().to("high-scores")

        return builder.build()
    }
}