package io.sdc.client.cache

import io.sdc.client.cache.model.Index
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer
import okio.*
import okio.ByteString.Companion.decodeBase64
import okio.ByteString.Companion.encodeUtf8
import okio.Path.Companion.toPath
import kotlin.io.use
import okio.FileSystem
import kotlin.math.ceil

@OptIn(InternalSerializationApi::class)
class Dataset<T : Any>(
    private val serializer: KSerializer<T>,
    fileName: String,
    shouldOverride: Boolean = false,
    readOnly: Boolean = false
){
    private lateinit var datasetFileName: String
    private val INDEX_SIZE = 13L
    private val MAX_THREADS: Int = 5
    private val MAX_INDEX_LOAD_COUNT: Int = 500
    private var datasetIndex: MutableMap<Int, Index> = HashMap()
    private var datasetStatus: DatasetStatus = DatasetStatus.NOT_OPENED
    private var lastError: Exception? = null
    private val dataExt: String = ".data"
    private val indexExt: String = ".idx"
    private val tempExt: String = ".tmp"
    private var dataFileSize: Long = 0;

    private fun initIndex(datasetFilePath: String? = null){
        val datasetPath = datasetFilePath?.toPath() ?: (datasetFileName + indexExt).toPath()
        val fileSize = getFileSize(datasetPath) ?: return
        val totalRecords: Int = (fileSize / INDEX_SIZE).toInt()

        val threadsNeeded: Int = ceil((totalRecords.toDouble() / MAX_INDEX_LOAD_COUNT)).toInt()

        val maxThreads = if(threadsNeeded >= MAX_THREADS){
            MAX_THREADS
        } else {
            threadsNeeded
        }

        val chunks = mutableListOf<List<Int>>()
        for (i in 0..<totalRecords step MAX_INDEX_LOAD_COUNT) {
            val end = (i + MAX_INDEX_LOAD_COUNT).coerceAtMost(totalRecords)
            chunks.add((i..<end).toList())
        }

        val arrayOfLists: Array<MutableList<List<Int>>> = Array(minOf(maxThreads, chunks.size)) { mutableListOf() }

        chunks.forEachIndexed { index, chunk ->
            val threadIndex = index % arrayOfLists.size
            arrayOfLists[threadIndex].add(chunk)
        }



        runBlocking {
            val jobs = mutableListOf<Job>()

            arrayOfLists.forEachIndexed { index, list ->
                val job = launch {
                    runIndexThread(list)
                }
                jobs.add(job)
            }
            jobs.forEach { job ->
                job.join()
            }
        }
    }

    private fun runIndexThread(list: MutableList<List<Int>>){
        for (subList in list) {
            for (index in subList) {
                val myInd = getRecordIndex(index * INDEX_SIZE)
                datasetIndex[datasetIndex.size] = Index(myInd!!.size, myInd.startPosition, myInd.status)
            }
        }
    }

    fun getDatasetStatus(): DatasetStatus {
        return datasetStatus
    }

    fun getLastError(): Exception?{
        return lastError
    }

    companion object {
        inline fun <reified T : Any> create(
            fileName: String,
            shouldOverride: Boolean = false,
            readOnly: Boolean = false
        ): Dataset<T> {
            val serializer = T::class.serializer()
            return Dataset(serializer, fileName, shouldOverride, readOnly)
        }
    }

    init {
        datasetFileName = fileName
        val fileSystem = FileSystem.SYSTEM

        if (fileSystem.exists((datasetFileName + dataExt).toPath())) {
            if (shouldOverride) {
                fileSystem.delete((datasetFileName + dataExt).toPath())
                fileSystem.write((datasetFileName + dataExt).toPath()) {
                    // File created but no content written
                }
            }
        } else {
            fileSystem.write((datasetFileName + dataExt).toPath()) {
                // File created but no content written
            }
        }

        if (fileSystem.exists((datasetFileName + indexExt).toPath())) {
            if (shouldOverride) {
                fileSystem.delete((datasetFileName + indexExt).toPath())
                fileSystem.write((datasetFileName + indexExt).toPath()) {
                    // File created but no content written
                }
            }
        } else {
            fileSystem.write((datasetFileName + indexExt).toPath()) {
                // File created but no content written
            }
        }

        initIndex()
        dataFileSize = if(getFileSize((datasetFileName + dataExt).toPath()) == null) {
            0
        } else {
            getFileSize((datasetFileName + dataExt).toPath())!!
        }
    }

    fun getAll(): Flow<Int> = flow {
        datasetIndex.forEach { (key, _) ->
            emit(key)
        }
    }.flowOn(Dispatchers.Default)

    fun update(index: Int, data: T){
        try {

            var recordStartPosition = 0L
            var indexStartPosition = 0L
            var lastIndex = getRecordIndex(0L)
            while(lastIndex != null){
                recordStartPosition += lastIndex.size
                indexStartPosition += INDEX_SIZE
                lastIndex = getRecordIndex(indexStartPosition)
            }

            val jsonString = Json.encodeToString(serializer, data).encodeUtf8().base64()

            var updateIndex = datasetIndex[index]
            if(updateIndex != null){
                updateRecordStatus(updateIndex.startPosition, RecordStatus.LOCKED)
                saveIndex(indexStartPosition, jsonString.length, RecordStatus.PENDING, dataFileSize)
                saveRecordData(recordStartPosition, jsonString)
                updateRecordStatus(indexStartPosition, RecordStatus.FINAL)
                updateRecordStatus(updateIndex.startPosition, RecordStatus.DELETED)
                datasetIndex[index] = Index(updateIndex.size, updateIndex.startPosition, RecordStatus.DELETED)
                datasetIndex[datasetIndex.size] = Index(jsonString.length, recordStartPosition, RecordStatus.FINAL)
                updateRecordDataNull(datasetIndex[index]!!.startPosition, datasetIndex[index]!!.size)

                dataFileSize += jsonString.length
            }
        } catch (e: Exception) {
            lastError = e
            throw e
        }
    }

    fun insert(data: T){
        try {
            var recordStartPosition = getFileSize((datasetFileName + dataExt).toPath()) ?: 0
            var indexStartPosition = getFileSize((datasetFileName + indexExt).toPath()) ?: 0

            val jsonString = Json.encodeToString(serializer, data).encodeUtf8().base64()

            saveIndex(indexStartPosition, jsonString.length, RecordStatus.PENDING, dataFileSize)
            saveRecordData(recordStartPosition, jsonString)
            updateRecordStatus(indexStartPosition, RecordStatus.FINAL)
            datasetIndex[datasetIndex.size] = Index(jsonString.length, recordStartPosition, RecordStatus.FINAL)

            dataFileSize += jsonString.length

        } catch (e: Exception) {
            lastError = e
            throw e
        }
    }

    fun delete(index: Int){
        try {
            var indexItem = datasetIndex[index]
            if(indexItem != null){
                updateRecordStatus(index * INDEX_SIZE, RecordStatus.DELETED)
                datasetIndex[index] = Index(indexItem.size, indexItem.startPosition,RecordStatus.DELETED)
                updateRecordDataNull(datasetIndex[index]!!.startPosition, datasetIndex[index]!!.size)
            }
        } catch (e: Exception) {
            lastError = e
            throw e
        }
    }


    fun get(index: Int): T? {
        try {
            var record: T? = null
            val indexDetails = datasetIndex[index]
            if (indexDetails != null) {
                if (indexDetails.status == RecordStatus.FINAL || indexDetails.status == RecordStatus.LOCKED){
                    record = getRecordData(indexDetails)
                }
            }
            return record

        } catch (e: Exception) {
            lastError = e
            throw e
        }
    }


    private fun updateRecordDataNull(offset: Long, recordSize: Int, datasetFilePath: String? = null){
        try {
            val datasetPath = datasetFilePath?.toPath() ?: (datasetFileName + dataExt).toPath()
            val datasetPathName = datasetFilePath ?: (datasetFileName + dataExt)

            if(datasetPathName.isEmpty()){
                lastError = Exception("Dataset Path is empty.")
                throw lastError as Exception
            }

            FileSystem.SYSTEM.openReadWrite(datasetPath).use { fileHandle ->
                val buffer = Buffer()
                buffer.write(ByteArray(recordSize))
                fileHandle.write(offset, buffer, buffer.size)
            }

        } catch (e: Exception) {
            lastError = e
            throw e
        }
    }

    private fun saveRecordData(offset: Long, data: String, datasetFilePath: String? = null){
        try {
            val datasetPath = datasetFilePath?.toPath() ?: (datasetFileName + dataExt).toPath()
            val datasetPathName = datasetFilePath ?: (datasetFileName + dataExt)

            if(datasetPathName.isEmpty()){
                lastError = Exception("Dataset Path is empty.")
                throw lastError as Exception
            }

            FileSystem.SYSTEM.openReadWrite(datasetPath).use { fileHandle ->
                val buffer = Buffer()
                buffer.writeUtf8(data)
                fileHandle.write(offset, buffer, buffer.size)
            }

        } catch (e: Exception) {
            lastError = e
            throw e
        }
    }

    private fun saveIndex(offset: Long, recordSize: Int, recordStatus: RecordStatus, recordStartPos: Long, datasetFilePath: String? = null){
        try {
            val datasetPath = datasetFilePath?.toPath() ?: (datasetFileName + indexExt).toPath()
            val datasetPathName = datasetFilePath ?: (datasetFileName + indexExt)

            if(datasetPathName.isEmpty()){
                lastError = Exception("Dataset Path is empty.")
                throw lastError as Exception
            }

            if(datasetPathName.isNotEmpty()){
                FileSystem.SYSTEM.openReadWrite(datasetPath).use { fileHandle ->
                    val buffer = Buffer()
                    buffer.writeByte(recordStatus.code.toInt())
                    buffer.writeInt(recordSize)
                    buffer.writeLong(recordStartPos)
                    fileHandle.write(offset, buffer, buffer.size)
                }
            } else {
                lastError = Exception("Dataset Path is empty.")
                throw lastError as Exception
            }
        } catch (e: Exception) {
            lastError = e
            throw e
        }
    }

    private fun getRecordData(index: Index, datasetFilePath: String? = null): T?{
        try {
            var record: T? = null
            val datasetPath = datasetFilePath?.toPath() ?: (datasetFileName + dataExt).toPath()
            val datasetPathName = datasetFilePath ?: (datasetFileName + dataExt)
            if(datasetPathName.isEmpty()){
                lastError = Exception("Dataset Path is empty.")
                throw lastError as Exception
            }
            FileSystem.SYSTEM.openReadWrite(datasetPath).use { fileHandle ->
                val buffer = Buffer()
                fileHandle.read(index.startPosition,buffer,index.size.toLong())
                if(buffer.size == index.size.toLong()){
                    val json: String = buffer.readUtf8()
                    record = json.decodeBase64()?.utf8()?.let {
                        Json.decodeFromString(serializer, it)
                    }
                }
            }

            return record
        } catch (e: Exception) {
            lastError = e
            throw e
        }
    }

    private fun getRecordIndex(recordPosition: Long, datasetFilePath: String? = null): Index?{
        try {
            val datasetPath = datasetFilePath?.toPath() ?: (datasetFileName + indexExt).toPath()
            val datasetPathName = datasetFilePath ?: (datasetFileName + indexExt)

            if(datasetPathName.isNotEmpty()){
                var index: Index? = null
                FileSystem.SYSTEM.openReadWrite(datasetPath).use { fileHandle ->
                    val buffer = Buffer()
                    fileHandle.read(recordPosition,buffer,INDEX_SIZE)
                    if(buffer.size == INDEX_SIZE){
                        val status: Byte = buffer.readByte()
                        val size: Int = buffer.readInt()
                        val startPos: Long = buffer.readLong()
                        index = RecordStatus.fromByte(status)?.let { Index(size, startPos, it) }
                    }
                }
                return index
            } else {
                lastError = Exception("Dataset Path is empty.")
                throw lastError as Exception
            }
        } catch (e: Exception) {
            lastError = e
            throw e
        }
    }


    private fun updateRecordStatus(startPosition: Long, status: RecordStatus, datasetFilePath: String? = null){
        try {
            val datasetPath = datasetFilePath?.toPath() ?: (datasetFileName + indexExt).toPath()
            val datasetPathName = datasetFilePath ?: (datasetFileName + indexExt)

            if(datasetPathName.isEmpty()){
                lastError = Exception("Dataset Path is empty.")
                throw lastError as Exception
            }

            if(datasetPathName.isNotEmpty()){
                FileSystem.SYSTEM.openReadWrite(datasetPath).use { fileHandle ->
                    val buffer = Buffer()
                    buffer.writeByte(status.code.toInt())
                    fileHandle.write(startPosition, buffer, buffer.size)
                }
            } else {
                lastError = Exception("Dataset Path is empty.")
                throw lastError as Exception
            }
        } catch (e: Exception) {
            lastError = e
            throw e
        }
    }

    private fun getFileSize(filePath: Path): Long? {
        try {
            val fileSystem = FileSystem.SYSTEM
            if(!fileSystem.exists(filePath)){
                return null
            }

            return FileSystem.SYSTEM.metadata(filePath).size ?: 0L
        } catch (e: Exception) {
            return null
        }
    }

    fun compact(){
        //todo
        try {
            val fileSystem = FileSystem.SYSTEM
            fileSystem.write((datasetFileName + dataExt + tempExt).toPath()) {
                // File created but no content written
            }

            fileSystem.write((datasetFileName + indexExt + tempExt).toPath()) {
                // File created but no content written
            }

            var tempFileSize: Long = 0;
            for ((key, value) in datasetIndex) {
                println("Key: $key, Value: $value")
                if(value.status == RecordStatus.LOCKED || value.status == RecordStatus.FINAL){
                    get(key)?.let {
                        val data: T = it
                        var recordStartPosition = getFileSize((datasetFileName + dataExt + tempExt).toPath()) ?: 0
                        var indexStartPosition = getFileSize((datasetFileName + indexExt + tempExt).toPath()) ?: 0

                        val jsonString = Json.encodeToString(serializer, data).encodeUtf8().base64()

                        saveIndex(indexStartPosition, jsonString.length, RecordStatus.PENDING, tempFileSize, (datasetFileName + indexExt + tempExt))
                        saveRecordData(recordStartPosition, jsonString, (datasetFileName + dataExt + tempExt))
                        updateRecordStatus(indexStartPosition, RecordStatus.FINAL, (datasetFileName + indexExt + tempExt))

                        tempFileSize += jsonString.length
                    }
                }
            }

            fileSystem.delete((datasetFileName + dataExt).toPath())
            fileSystem.copy((datasetFileName + dataExt + tempExt).toPath(), (datasetFileName + dataExt).toPath())
            fileSystem.delete((datasetFileName + dataExt + tempExt).toPath())

            fileSystem.delete((datasetFileName + indexExt).toPath())
            fileSystem.copy((datasetFileName + indexExt + tempExt).toPath(), (datasetFileName + indexExt).toPath())
            fileSystem.delete((datasetFileName + indexExt + tempExt).toPath())

            datasetIndex.clear()

        } catch (e: Exception){
            //todo
        }
    }
}

