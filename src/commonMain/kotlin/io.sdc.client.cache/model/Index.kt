package io.sdc.client.cache.model

import io.sdc.client.cache.RecordStatus
import kotlinx.serialization.Serializable

@Serializable
data class Index(var size: Int,var startPosition: Long, var status: RecordStatus) {
}