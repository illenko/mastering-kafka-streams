package com.example.tweets

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class TweetsApplication

fun main(args: Array<String>) {
	runApplication<TweetsApplication>(*args)
}
