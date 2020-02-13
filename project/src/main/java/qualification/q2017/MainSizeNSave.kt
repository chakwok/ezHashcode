package qualification.q2017

import java.io.File
import java.util.*

// just for quick access here
lateinit var videoToSize: HashMap<Int, Int>;
lateinit var servers: Array<CacheServer>;
var minVideoSize = Int.MAX_VALUE;

fun main() {
    execute("project/src/main/java/qualification/q2017/qualification_round_2017.in/kittens.in.txt")
    execute("project/src/main/java/qualification/q2017/qualification_round_2017.in/me_at_the_zoo.in")
    execute("project/src/main/java/qualification/q2017/qualification_round_2017.in/trending_today.in")
    execute("project/src/main/java/qualification/q2017/qualification_round_2017.in/videos_worth_spreading.in")
}

private fun execute(pathname: String) {
    //parsing the input
//    val pathname = "project/src/main/java/qualification/q2017/qualification_round_2017.in/small.in"
    val file = File(pathname)
    val sc = Scanner(file)

    val numVideo = sc.nextInt();
    val numEndpoint = sc.nextInt();
    val numRequest = sc.nextInt();
    val numServers = sc.nextInt();
    val cacheSize = sc.nextInt();


    videoToSize = HashMap<Int, Int>(numVideo)
    for(i in 0 until numVideo) {
        val size = sc.nextInt();
        videoToSize[i] = size;
        minVideoSize = Math.min(size, minVideoSize);
    }
    println(videoToSize)

    // will be converted to pq after initializing
    val endpoints = ArrayList<Endpoint>(numEndpoint);
    for(i in 0 until numEndpoint) {
        val dbLatency = sc.nextInt();
        val size = sc.nextInt();
        var thisEndpoint = Endpoint(dbLatency);

        var maxSave = Int.MIN_VALUE;
        for(j in 0 until size) {
            val pos = sc.nextInt();
            var saved = (dbLatency - sc.nextInt())

//            thisEndpoint.latencyToMachine[pos] = saved;
            val machines = thisEndpoint.latencySavedToMachines.getOrDefault(saved, ArrayList<Int>());
            machines.add(pos);
            thisEndpoint.latencySavedToMachines[saved] = machines;
            maxSave = Math.max(maxSave, saved)
        }
        thisEndpoint.maxSave = maxSave;
        endpoints.add(thisEndpoint);
    }

    endpoints.forEach {println(it)}

    for(i in 0 until numRequest) {
        val videoId = sc.nextInt();
        val endpointId = sc.nextInt();
        val numReq = sc.nextInt();
        val size = numReq * videoToSize[videoId]!!
        val endpoint = endpoints[endpointId]
        val videos = endpoint.sizeToVids.getOrDefault(size,  ArrayList<Int>());
        videos.add(videoId)
        endpoint.sizeToVids[size] = videos;
        endpoint.maxSize= Math.max(size, endpoint.maxSize)
        endpoint.totalSize += size;
    }

    servers = Array<CacheServer>(numServers) {CacheServer(cacheSize)}

    //this Comparator is the most important of the program
    //ideally it should contain a heuristic to find the best video to put in the best server
    val pq = PriorityQueue<Endpoint>(numEndpoint) { o1, o2 ->
        (o2.maxSize * o2.maxSave - o1.maxSize * o1.maxSave)
    }
//    val pq = PriorityQueue<Endpoint>(numEndpoint, compareBy(Endpoint::maxSize))
    pq.addAll(endpoints);
    val cachedVideo = HashSet<Int>()
    //start the dequeue process to put video in cacheServer
    var endpoint = pq.poll();
    while(endpoint != null/* && endpoint.maxSize > 0*/) {
        val map = endpoint.sizeToVids;
        if(map.size == 0) {
            endpoint = pq.poll()
            continue;
        }
        var lastKey = map.lastKey();
        val list = map.remove(lastKey);
        if(list == null) {
            endpoint = pq.poll()
            continue;
        }
        val videoToPut = list[0];
        if(cachedVideo.contains(videoToPut)) {
            if(map.size != 0 && endpoint.maxSave > 0) {
                lastKey = map.lastKey();
                endpoint.maxSize = lastKey;
                pq.offer(endpoint);
            }
            pq.poll();
            continue;
        }
//        var machinePutInto = putVideoToAvailableServer(videoToPut, endpoint.latencySavedToMachines)

//    var serverss = latencySavedToMachines.iterator();
//    while(serverss.hasNext()) {
        var latencySavedToMachines =endpoint.latencySavedToMachines;
        if(latencySavedToMachines.size == 0) {
            endpoint = pq.poll()
            continue
        }

        val machines = latencySavedToMachines.remove(latencySavedToMachines.firstKey());
        if(machines == null) {
            endpoint = pq.poll();
            continue;
        }

        val videoSize = videoToSize[videoToPut];
        var maxSpaceLeft= 0;
        var bestMachine= -1;
        for(machine in machines) {
            if(servers[machine].available && servers[machine].spaceLeft > videoSize!!) {
                if(servers[machine].spaceLeft > maxSpaceLeft) {
                    maxSpaceLeft = Math.max(maxSpaceLeft, servers[machine].spaceLeft)
                    bestMachine = machine;
                }
            }
        }
        if(bestMachine !=-1) {
            putVideoToServer(videoToPut, videoSize!!, bestMachine)
            cachedVideo.add(videoToPut)
        }

        if(latencySavedToMachines.size > 0) {
            val first = latencySavedToMachines.firstKey();
            if(first > 0) {
                endpoint.maxSave = first
            } else {
                endpoint = pq.poll();
                continue;
            }
        } else {
            endpoint = pq.poll();
            continue;
        }


        if(map.size != 0 && endpoint.maxSave > 0) {
            lastKey = map.lastKey();
            endpoint.maxSize = lastKey;
            pq.offer(endpoint);
        }
//        if(endpoint.sizeToVids.size >0) {
            endpoint = pq.poll()

//        }
    }


    var size = 0;
    servers.forEach {
        if(it.spaceLeft != it.space) {
            size++;
            println(it.videos)
        }
    };

    val out = pathname.split("/");
    val outname = out[out.lastIndex]

    File("$outname.txt").printWriter().use { out ->
        out.println(size)
        for(i in servers.indices) {
            if(servers[i].videos.size != 0) {
                out.println("$i ${servers[i].videos.joinToString(" ")}")
            }
        }

     }
}

fun putVideoToAvailableServer(videoId: Int, latencySavedToMachines: TreeMap<Int, MutableList<Int>>): Int {
    if(latencySavedToMachines.size == 0) {
        return -1;
    }
//    var serverss = latencySavedToMachines.iterator();
//    while(serverss.hasNext()) {
    val machines = latencySavedToMachines.remove(latencySavedToMachines.firstKey());
    if(machines == null) {
        return -1;
    }

    val videoSize = videoToSize[videoId];
    var maxSpaceLeft= 0;
    var bestMachine= -1;
    for(machine in machines) {
        if(servers[machine].available && servers[machine].spaceLeft > videoSize!!) {
            if(servers[machine].spaceLeft > maxSpaceLeft) {
                bestMachine = machine;
            }
        }
        if(bestMachine !=-1) {
            putVideoToServer(videoId, videoSize!!, bestMachine)
            return machine;
        }
    }
//    }
    return -1;
}


fun putVideoToServer(videoId:Int, sizeInt: Int, machine: Int) {
    println("putting video $videoId to server $machine")
    var thisServer = servers[machine];
    thisServer.videos.add(videoId);
    thisServer.spaceLeft -= sizeInt;
    if(thisServer.spaceLeft < minVideoSize) {
        thisServer.available = false;
    }
}

data class Endpoint(
        val dbLatency: Int
) {
    val latencySavedToMachines = TreeMap<Int, MutableList<Int>>(reverseOrder())
    var maxSave: Int = Int.MIN_VALUE;
    val sizeToVids = TreeMap<Int, MutableList<Int>>();
    var maxSize = 0;
    var totalSize = 0;
}


data class CacheServer(
    val space: Int
) {
    var spaceLeft = space;
    var available = true;
    val videos = ArrayList<Int>();
}