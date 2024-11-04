package com.example.leaderboard.service

import com.example.leaderboard.domain.Enriched
import com.example.leaderboard.domain.HighScores
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import okhttp3.OkHttpClient
import okhttp3.Request
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.HostInfo
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.springframework.stereotype.Service

@Service
class LeaderboardService(
    private val hostInfo: HostInfo,
    private val streams: KafkaStreams,
    private val objectMapper: ObjectMapper,
) {

    fun getStore(): ReadOnlyKeyValueStore<String, HighScores?> =
        streams.store(
            StoreQueryParameters.fromNameAndType(
                "leader-boards",
                QueryableStoreTypes.keyValueStore()
            )
        )

    fun getKey(productId: String): List<Enriched>? {
        println("Starting getKey for productId: $productId")
        val metadata = streams.queryMetadataForKey("leader-boards", productId, Serdes.String().serializer())
        println("Metadata for key $productId: $metadata")

        if (hostInfo == metadata.activeHost()) {
            println("Querying local store for key: $productId")
            val result = getStore().get(productId)?.toList()
            println("Local store result for key $productId: $result")
            return result
        }

        val remoteHost = metadata.activeHost().host()
        val remotePort = metadata.activeHost().port()
        val url = "http://$remoteHost:$remotePort/leaderboard/$productId"
        println("Querying remote store at URL: $url")

        val client = OkHttpClient()
        val request = Request.Builder().url(url).build()

        client.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                println("Failed to fetch data from remote store for key $productId")
                throw RuntimeException("Failed to fetch data from remote store")
            }
            val responseBody = response.body?.string() ?: throw RuntimeException("Response body is null")
            println("Response body for key $productId: $responseBody")
            val result = objectMapper.readValue(responseBody, object : TypeReference<List<Enriched>>() {})
            println("Parsed result for key $productId: $result")
            return result
        }
    }
}