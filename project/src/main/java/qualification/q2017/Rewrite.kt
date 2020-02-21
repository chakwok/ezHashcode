package qualification.q2017.b

import java.io.File
import java.util.*
import java.util.concurrent.ConcurrentSkipListMap


lateinit var videoIdToSize: HashMap<String, Int>
lateinit var cacheServers: Array<CacheServer>;
lateinit var endpoints: MutableList<Endpoint>;
var count = 0;

fun main() {
    qualification.q2017.b.execute("project/src/main/java/qualification/q2017/qualification_round_2017.in/me_at_the_zoo.in")
    qualification.q2017.b.execute("project/src/main/java/qualification/q2017/qualification_round_2017.in/videos_worth_spreading.in")
    qualification.q2017.b.execute("project/src/main/java/qualification/q2017/qualification_round_2017.in/trending_today.in")
    qualification.q2017.b.execute("project/src/main/java/qualification/q2017/qualification_round_2017.in/kittens.in.txt")

}

private fun execute(pathname: String) {
    println("start executing on $pathname")
    count = 0;
    //parsing the input
//    val pathname = "project/src/main/java/qualification/q2017/qualification_round_2017.in/small.in"
    val file = File(pathname)
    val sc = Scanner(file)

    val numVideo = sc.nextInt();
    val numEndpoint = sc.nextInt();
    val numRequest = sc.nextInt();
    val numServers = sc.nextInt();
    val cacheSize = sc.nextInt();

    videoIdToSize = HashMap<String, Int>(numVideo)
    for(i in 0 until numVideo) {
        val size = sc.nextInt();
        videoIdToSize[i.toString()] = size;
    }

    endpoints = ArrayList<Endpoint>(numEndpoint);
    for(i in 0 until numEndpoint) {
        val dbLatency = sc.nextInt();
        val size = sc.nextInt();
        val thisEndpoint = Endpoint(i.toString(), dbLatency);

        var potentialSave = Int.MIN_VALUE;
        for(j in 0 until size) {
            val cacheServerId = sc.nextInt().toString();
            var latency = sc.nextInt()

            val cacheServersLatency = thisEndpoint.cacheServersLatency;
            val thisLatencyServers = cacheServersLatency.getOrDefault(latency, ArrayList<String>())
            thisLatencyServers.add(cacheServerId)
            cacheServersLatency[latency] = thisLatencyServers;
        }
        endpoints.add(thisEndpoint);
    }

//    endpoints.forEach {println(it)}

    for(i in 0 until numRequest) {
        val videoId = sc.nextInt().toString()
        val endpointId = sc.nextInt();
        val numReq = sc.nextInt();
        val endpoint = endpoints[endpointId]
        val thisRequest = Request(endpointId.toString(), videoId, endpoint.dbLatency, numReq);
        endpoint.requests.add(thisRequest)
    }

    cacheServers = Array<CacheServer>(numServers) {CacheServer(it.toString(), cacheSize)}

    var pair = getIdOfRequestToOptimized(endpoints);
    var idOfEndpoint= pair.first;
    var idOfRequestToOptimized: UUID? = pair.second;
    while(idOfEndpoint != null && idOfRequestToOptimized != null) {
        val pair2 = optimizeARequest(idOfEndpoint, idOfRequestToOptimized);
        val videoIdPut = pair2.first;
        val serverPutIn = pair2.second;
        if(serverPutIn != null) {
            putVideoToServerAndUpdateEndpoints(videoIdPut, serverPutIn);
        }

        pair = getIdOfRequestToOptimized(endpoints);
        idOfEndpoint= pair.first;
        idOfRequestToOptimized = pair.second;
    }


    // output
    var size = 0;
    cacheServers.forEach {
        if(it.spaceAvailable != it.space) {
            size++;
            println(it.videos)
        }
    };

    val out = pathname.split("/");
    val outname = out[out.lastIndex]

    File("$outname.txt").printWriter().use { out ->
        out.println(size)
        for(i in cacheServers.indices) {
            if(cacheServers[i].videos.size != 0) {
                out.println("$i ${cacheServers[i].videos.joinToString(" ")}")
            }
        }
    }


}

fun putVideoToServerAndUpdateEndpoints(videoId: String, cacheServerId: String) {
    var thisServer = cacheServers[cacheServerId.toInt()];
    thisServer.videos.add(videoId);
    thisServer.spaceAvailable -= videoIdToSize[videoId]!!;

    for(endpoint in endpoints) {
        for(request in endpoint.requests) {
            if(!request.optimized && request.videoId == videoId) {
                for(entry in endpoint.cacheServersLatency) {
                    if(entry.value.contains(cacheServerId) && entry.key < request.latency) {
                        request.latency = entry.key;
                        if(entry.key == endpoint.cacheServersLatency.firstKey()) {
                            request.optimized = true;
                        }
                        break;
                    }
                }
            }
        }
    }
}

fun optimizeARequest(endpointId: String, requestId: UUID): Pair<String, String?> { // videoId, serverId
    println("optimized ${++count} requests")
    val endpoint = endpoints[endpointId.toInt()]
    val request = endpoint.requests.filter { it.uuid == requestId }[0]
    request.optimized = true; // will be true before returning anyway

    val serverIterator = endpoint.cacheServersLatency.iterator();
    var bestServerId: String? = null;

    while(serverIterator.hasNext()) {
        val entry = serverIterator.next();
        var bestServers: List<String> = entry.value;
        var maxSpaceLeft: Int = 0;
        for(serverId in bestServers) {
            val serverInfo = cacheServers[serverId.toInt()]
            if(serverInfo.spaceAvailable > videoIdToSize[request.videoId]!!) {
                if(serverInfo.spaceAvailable > maxSpaceLeft) {
                    bestServerId = serverId;
                    maxSpaceLeft = serverInfo.spaceAvailable
                }
            }
        }
        if(bestServerId != null) {
            return Pair(request.videoId, bestServerId)
        }
    }
    return Pair(request.videoId, bestServerId);

}


fun getIdOfRequestToOptimized(endpoints: MutableList<Endpoint> ): Pair<String?, UUID?> {
//    var endpointId: String,
    var idOfRequestToOptimized: UUID? = null;
    var idOfEndpointId: String?= null;
    var maxSize = Int.MIN_VALUE;
    for(endpoint in endpoints) {
        for(request in endpoint.requests) {
//            println(request.videoId)
            val size = request.volume * videoIdToSize[request.videoId]!!;
            if(!request.optimized && size > maxSize) {
                idOfRequestToOptimized = request.uuid;
                idOfEndpointId = endpoint.id;
                maxSize = size;
            }

        }
    }
    return Pair(idOfEndpointId, idOfRequestToOptimized);
}


data class Endpoint(
        val id: String,
        val dbLatency: Int
) {
    val requests: MutableList<Request> = ArrayList();
    val cacheServersLatency: ConcurrentSkipListMap<Int, MutableList<String>> = ConcurrentSkipListMap()


}

data class Request(
        val from: String,
        val videoId: String,
        var latency: Int,
        val volume: Int
) {
    var optimized: Boolean = false;
    var uuid: UUID = UUID.randomUUID();

}

data class CacheServer(
        val id: String,
        val space: Int
) {
    var spaceAvailable = space;
    val videos = ArrayList<String>();

}