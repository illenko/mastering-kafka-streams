package com.example.leaderboard.domain

import java.util.TreeSet

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

data class HighScores(
    val highScores: TreeSet<Enriched> = TreeSet()
) {
    fun add(enriched: Enriched): HighScores {
        highScores.add(enriched)
        if (highScores.size > 3) {
            highScores.remove(highScores.last())
        }
        return this
    }

    fun toList(): List<Enriched> = highScores.toList()
}