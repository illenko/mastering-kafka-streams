package com.example.leaderboard.api

import com.example.leaderboard.service.LeaderboardService
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController

@RestController
class LeaderboardController(
    private val leaderboardService: LeaderboardService,
) {

    @GetMapping("/leaderboard/{productId}")
    fun getLeaderboard(@PathVariable productId: String) = leaderboardService.getKey(productId)

}