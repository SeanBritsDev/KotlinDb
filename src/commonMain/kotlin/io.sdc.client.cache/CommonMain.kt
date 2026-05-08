package io.sdc.client.cache

import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import java.time.LocalDateTime

@Serializable
data class User(val name: String, val age: Int)

fun mainHello() {
    val time0 = LocalDateTime.now()
    println("begin $time0")

    var dataset = Dataset.create<User>(
        fileName = "dataset",
        shouldOverride = false,
        readOnly = false
    )


    var user1 = User("Sean-Lee Brits",23)
    var user2 = User("Roydon Brits the second",11)
//    val jsonString1 = Json.encodeToString(user1).encodeUtf8().base64()
//    val jsonString2 = Json.encodeToString(user2).encodeUtf8().base64()

//    dataset.saveRecordData(40L, jsonString1)
//    dataset.saveIndex(5L,40, RecordStatus.FINAL)
//    dataset.delete(0)
//    dataset.update(0, user1)
//    dataset.insert(user2)
//    dataset.insert(user1)
//    dataset.insert(user1)
//    dataset.insert(user1)
//    dataset.insert(user1)
//    dataset.insert(user1)
//    dataset.insert(user1)
//    dataset.insert(user1)
//    dataset.insert(user1)
//    dataset.insert(user1)
//    dataset.insert(user1)

//    val time1 = LocalDateTime.now()
//    println("Start $time1")

//    for (i in 20..<50) {
//        dataset.delete(i)
//    }

//    val time2 = LocalDateTime.now()
//    println("Mid $time2")

//    for (i in 100..<10000) {
//        dataset.insert(User("Name and age -> ",i))
//    }

//    val time3 = LocalDateTime.now()
//    println("Done $time3")

//    for (i in 0..<10) {
//        println(dataset.get(i))
//    }


//    dataset.delete(1)

//    dataset.compact()

    runBlocking {
        dataset.getAll().collect { key ->
            println("${dataset.get(key)}")
        }
    }

}