package io.sdc.client.cache

enum class DatasetStatus(val code: String) {
    NOT_OPENED("N"),
    LOAD("L"),
    READ_ONLY("R"),
    WRITABLE("W"),
    COMPACTING("C"),
    ERROR("E");

    companion object {
        private val map = DatasetStatus.values().associateBy(DatasetStatus::code)
        fun fromString(type: String) = map[type]
    }
}