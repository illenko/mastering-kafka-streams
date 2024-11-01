package com.example.leaderboard.streams

import com.example.leaderboard.domain.*
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Joined
import org.apache.kafka.streams.kstream.ValueJoiner
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

        ): Topology {

        val scoreEvents = builder.stream("score-events", Consumed.with(Serdes.String(), scoreEventSerde))
            .selectKey { _, scoreEvent -> scoreEvent.playerId.toString() }

        val players = builder.table("players", Consumed.with(Serdes.String(), playerSerde))

        val products = builder.globalTable("products", Consumed.with(Serdes.String(), productSerde))

        val scorePlayerJoiner = ValueJoiner<ScoreEvent, Player, ScoreWithPlayer> { s, p -> ScoreWithPlayer(s, p) }

        val productJoiner = ValueJoiner<ScoreWithPlayer, Product, Enriched> { s, p -> Enriched(s, p) }

        val playerJoinedParams = Joined.with(Serdes.String(), scoreEventSerde, playerSerde)

        val withPlayers = scoreEvents.join(players, scorePlayerJoiner, playerJoinedParams)


        return builder.build()
    }
}