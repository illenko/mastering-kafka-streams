package com.example.leaderboard.service

import com.example.leaderboard.domain.Enriched
import com.example.leaderboard.domain.HighScores
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import okhttp3.OkHttpClient
import okhttp3.Request
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyQueryMetadata
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.HostInfo
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.stereotype.Service

@Service
class LeaderboardService(
    private val hostInfo: HostInfo,
    private val factoryBean: StreamsBuilderFactoryBean,
    private val objectMapper: ObjectMapper,
) {

    fun getStore(): ReadOnlyKeyValueStore<String, HighScores?> =
        factoryBean.kafkaStreams?.store(
            StoreQueryParameters.fromNameAndType(
                "leader-boards",
                QueryableStoreTypes.keyValueStore()
            )
        ) ?: throw RuntimeException("KafkaStreams is not initialized")

    fun getKey(productId: String): List<Enriched>? {
        println("Starting getKey for productId: $productId")
        val metadata = getMetadataForKey(productId)
        println("Metadata for key $productId: $metadata")

        return if (hostInfo == metadata.activeHost()) {
            queryLocalStore(productId)
        } else {
            queryRemoteStore(metadata, productId)
        }
    }

    private fun getMetadataForKey(productId: String) =
        factoryBean.kafkaStreams?.queryMetadataForKey("leader-boards", productId, Serdes.String().serializer())
            ?: throw RuntimeException("KafkaStreams is not initialized")

    private fun queryLocalStore(productId: String): List<Enriched>? {
        println("Querying local store for key: $productId")
        val result = getStore().get(productId)?.toList()
        println("Local store result for key $productId: $result")
        return result
    }

    private fun queryRemoteStore(metadata: KeyQueryMetadata, productId: String): List<Enriched>? {
        val url = buildRemoteUrl(metadata, productId)
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

    private fun buildRemoteUrl(metadata: KeyQueryMetadata, productId: String): String {
        val remoteHost = metadata.activeHost().host()
        val remotePort = metadata.activeHost().port()
        return "http://$remoteHost:$remotePort/leaderboard/$productId"
    }
}