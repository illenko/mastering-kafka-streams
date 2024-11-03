package com.example.leaderboard.service

import com.example.leaderboard.domain.Enriched
import com.example.leaderboard.domain.HighScores
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
        val metadata = streams.queryMetadataForKey("leader-boards", productId, Serdes.String().serializer())

        getStore().get(productId)?.toList()?.let {
            println("Querying local store for key: $productId")
            return it
        }

        if (hostInfo == metadata.activeHost()) {
            println("Querying local store for key: $productId")
            return getStore().get(productId)?.toList()
        }

        val remoteHost = metadata.activeHost().host()
        val remotePort = metadata.activeHost().port()
        val url = "http://$remoteHost:$remotePort/leaderboard/$productId"

        val client = OkHttpClient()
        val request = Request.Builder().url(url).build()


        client.newCall(request).execute().use { response ->
            if (!response.isSuccessful) throw RuntimeException("Failed to fetch data from remote store")
            println("Response from $url: ${response.body?.string()}")
            return null
        }

    }


}