package com.example.monitoring.domain

interface Vital {
    val timestamp: String
}

data class Pulse(override val timestamp: String) : Vital

data class BodyTemp(
    override val timestamp: String,
    val temperature: Double,
    val unit: String
) : Vital

data class CombinedVitals(
    val heartRate: Int,
    val bodyTemp: BodyTemp
)