package io.sdc.client.cache

enum class RecordStatus(val code: Byte) {

    DELETED(1),
    LOCKED(2),
    FINAL(3),
    PENDING(4);

    companion object {
        private val map = RecordStatus.values().associateBy(RecordStatus::code)
        fun fromByte(type: Byte) = map[type]
    }
}

