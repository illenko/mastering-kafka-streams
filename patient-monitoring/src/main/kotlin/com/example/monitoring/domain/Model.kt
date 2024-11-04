package com.example.monitoring.domain

interface Vital {
    fun getTimestamp(): String
}

class Pulse(private val timestamp: String) : Vital {
    override fun getTimestamp(): String = timestamp
}