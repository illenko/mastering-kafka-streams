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