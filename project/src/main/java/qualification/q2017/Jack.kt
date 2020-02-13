import qualification.q2017.servers
import java.util.*
import java.io.*
import java.util.Map
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.HashSet
import kotlin.math.*

fun main() {
    execute("project/src/main/java/qualification/q2017/qualification_round_2017.in/kittens.in.txt")
    execute("project/src/main/java/qualification/q2017/qualification_round_2017.in/me_at_the_zoo.in")
    execute("project/src/main/java/qualification/q2017/qualification_round_2017.in/trending_today.in")
    execute("project/src/main/java/qualification/q2017/qualification_round_2017.in/videos_worth_spreading.in")
}

fun execute(inputFile: String){
//    val inputFile = "/Users/Jack/Desktop/d.txt"
    val sc = Scanner(FileReader(inputFile))

    val numVideos = sc.nextInt()
    val numEndpoints = sc.nextInt()
    val numRequests = sc.nextInt()
    val numCaches = sc.nextInt()
    val cacheSize = sc.nextInt()

    // Cache Server Index, <Video Index, Sum of reduced latency>
    val cacheVideoLatencyMap = HashMap<Int, TreeMap<Int, Long>>(numCaches)

    val videoSizeMap = HashMap<Int, Int>(numVideos)
    for(i in 0 until numVideos) {
        videoSizeMap[i] = sc.nextInt()
    }

    val endPoints = Array<EndPoint?>(numEndpoints){null}
    for(i in 0 until numEndpoints){
        val dcLatency = sc.nextInt()
        val numCaches = sc.nextInt()

        val endPoint = EndPoint(dcLatency, numCaches)
        for(n in 0 until numCaches){
            val cacheServerIndex = sc.nextInt()
            val cacheLatency = sc.nextInt()
            endPoint.insertCacheServer(cacheServerIndex, cacheLatency)
        }
        endPoints[i] = endPoint
    }

    val cacheServers = Array<CacheServer?>(numCaches){null}
    for(i in 0 until numCaches){
        cacheServers[i] = CacheServer(cacheSize)
        cacheVideoLatencyMap[i] = TreeMap()
    }

    val requests = Array<Request?>(numRequests){null}
    for(i in 0 until numRequests){
        val videoIndex = sc.nextInt()
        val endPointIndex = sc.nextInt()
        val endpoint = endPoints[endPointIndex]!!
        val numRequests = sc.nextInt()

        endpoint.getAllCacheServerIndexes().forEach { cacheServerIndex ->
            val videoLatencyMap = cacheVideoLatencyMap[cacheServerIndex]!!
            if(!videoLatencyMap.containsKey(videoIndex)){
                videoLatencyMap[videoIndex] = 0
            }

            videoLatencyMap[videoIndex] = videoLatencyMap[videoIndex]!! + (endpoint.dcLatency - endpoint.getCacheServerLatency(cacheServerIndex)) * numRequests
        }


        requests[i] = Request(videoIndex, endpoint, numRequests)
    }

    val cachedVideo = HashSet<Int>()
    cacheVideoLatencyMap.keys.forEach { cacheServerIndex ->
        val videoLatencyMap = cacheVideoLatencyMap[cacheServerIndex]!!

        cachedVideo.forEach{ cachedVideoIdex ->
            videoLatencyMap.remove(cachedVideoIdex)
        }

        val sortedList = videoLatencyMap.entries.sortedBy { it.value }.asReversed()

        for(i in 0 until sortedList.size){
            val videoIndex = sortedList[i].key
            val videoSize = videoSizeMap[videoIndex]!!
            val cacheServer = cacheServers[cacheServerIndex]!!

            if(cacheServer.setCacheAvailable(videoSize)) {
                cachedVideo.add(videoIndex)
                cacheServer.setCachedVideo(videoIndex, videoSize)
            }
        }
    }

    val size = cacheServers.filter {
        it!!.totalCachedSize != 0;
    }.size

    val out = inputFile.split("/");
    val outname = out[out.lastIndex]

    File("$outname.txt").printWriter().use { out ->
        out.println(size)
        for(i in cacheServers.indices) {
            if(cacheServers[i]!!.totalCachedSize != 0) {
                out.println("$i ${cacheServers[i]!!.cachedVideos.joinToString(" ")}")
            }
        }

    }




    var total: Double = 0.0
    var subTotal: Double = 0.0
    for(i in 0 until numRequests){
        total += requests[i]!!.getMark(cacheServers)
        subTotal += requests[i]!!.numRequests
    }

    println(total / subTotal * 1000.0)
}

data class EndPoint(val dcLatency: Int, val numCaches: Int){
    private val cacheServerLatencyMap = HashMap<Int, Int>(numCaches)

    fun insertCacheServer(index: Int, latency: Int){
        cacheServerLatencyMap[index] = latency
    }

    fun getAllCacheServerIndexes(): Set<Int>{
        return cacheServerLatencyMap.keys
    }

    fun getCacheServerLatency(index: Int): Int{
        return cacheServerLatencyMap[index]!!
    }

    // For testing
    fun getMark(videoIndex: Int, cacheServers: Array<CacheServer?>): Int{
        var latency = dcLatency
        var cacheServerIndex = -1

        cacheServerLatencyMap.keys.forEach {
            if(cacheServers[it]!!.isCachedVideo(videoIndex)){
                if(latency > cacheServerLatencyMap[it]!!){
                    latency = cacheServerLatencyMap[it]!!
                    cacheServerIndex = it
                }
            }
        }

        //println("$videoIndex, $cacheServerIndex, ${dcLatency-latency}")
        return dcLatency - latency
    }
}

data class CacheServer(val maxSize: Int){
    val cachedVideos = HashSet<Int>()
    var totalCachedSize = 0

    fun setCachedVideo(index: Int, size: Int){
        cachedVideos.add(index)
        totalCachedSize += size
    }

    fun setCacheAvailable(size: Int): Boolean{
        return totalCachedSize + size <= maxSize
    }

    fun isCachedVideo(index: Int): Boolean{
        return cachedVideos.contains(index)
    }
}

data class Request(val videoIndex: Int, val endPoint: EndPoint, val numRequests: Int){
    // For testing
    fun getMark(cacheServers: Array<CacheServer?>): Int{
        return endPoint.getMark(videoIndex, cacheServers) * numRequests
    }
}