package com.example.leaderboard.domain

data class Player(
    val id: Long,
    val name: String
)

data class Product(
    val id: Long,
    val name: String
)

data class ScoreEvent(
    val playerId: Long,
    val productId: Long,
    val score: Double
)

data class ScoreWithPlayer(
    val scoreEvent: ScoreEvent,
    val player: Player
)

data class Enriched(
    val playerId: Long,
    val productId: Long,
    val playerName: String,
    val gameName: String,
    val score: Double
) : Comparable<Enriched> {

    constructor(s: ScoreWithPlayer, p: Product) : this(
        playerId = s.player.id,
        productId = p.id,
        playerName = s.player.name,
        gameName = p.name,
        score = s.scoreEvent.score
    )

    override fun compareTo(other: Enriched): Int = other.score.compareTo(score)
}
