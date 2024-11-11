package com.example.model

enum class Power {
    ON,
    OFF,
}

enum class Type {
    DESIRED,
    REPORTED,
}

data class State(
    val timestamp: String,
    val windSpeedMph: Double,
    val power: Power,
    val type: Type,
)

data class DigitalTwin(
    var desired: State? = null,
    var reported: State? = null,
)
