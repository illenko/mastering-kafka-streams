package com.example.tweets.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
data class Tweet(
    val createdAt: Long,
    val id: Long,
    val text: String,
    val lang: String,
    val retweet: Boolean,
    val source: String
)