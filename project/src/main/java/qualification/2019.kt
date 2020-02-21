package qualification

import java.io.File
import java.util.*
import java.util.concurrent.ConcurrentSkipListSet
import kotlin.collections.HashSet


fun main() {
    execute("/Users/akwok/workspace/xzHashcode/project/src/main/java/qualification/a_example.txt")
    execute("/Users/akwok/workspace/xzHashcode/project/src/main/java/qualification/b_read_on.txt")

    execute("/Users/akwok/workspace/xzHashcode/project/src/main/java/qualification/e_so_many_books.txt")
    execute("/Users/akwok/workspace/xzHashcode/project/src/main/java/qualification/f_libraries_of_the_world.txt")
    execute("/Users/akwok/workspace/xzHashcode/project/src/main/java/qualification/c_incunabula.txt")
    execute("/Users/akwok/workspace/xzHashcode/project/src/main/java/qualification/d_tough_choices.txt")
}

val scanned = HashSet<Int>()
val libOrder = LinkedList<Int>();
val scheduledLib = ConcurrentSkipListSet<Lib>(compareBy(Lib::M).thenComparingInt(Lib::id).reversed())
var B =0// numBooks
var L=0// numLib
var D=0// numDays
lateinit var libs: ArrayList<Lib>

fun execute(pathname: String) {
    scanned.clear()
    libOrder.clear()
    scheduledLib.clear();

    println("start executing $pathname");
    val scores: IntArray //scores

    val sc = Scanner(File(pathname))
    B = sc.nextInt()// numBooks
    L = sc.nextInt()// numLib
    D = sc.nextInt()// numDays
    scores = IntArray(B)
    for (i in 0 until B) {
        scores[i] = sc.nextInt()
    }
    libs = ArrayList<Lib>(L)
    for (i in 0 until L) {
        val l = Lib(i, sc.nextInt(),sc.nextInt(),sc.nextInt())

        for (j in 0 until l.N) {
            val bookId = sc.nextInt()
            val score = scores[bookId];
            val book =  Book(bookId, score)
            l.books.add(book)
        }
        var effectiveDays = (D - l.T) * l.M
        val bookIter = l.books.iterator();
        while(bookIter.hasNext() && effectiveDays > 0) {
            val book = bookIter.next();
            l.sumScore += book.score
            effectiveDays--;
        }
        libs.add(l)
    }
    var toSignUp = scheduleForSignup();
    var signUpDay = signUpALib(toSignUp)
    for(i in 0 until D) {
        if(signUpDay == 0) {
            if(toSignUp != -1) {
                scheduledLib.add(libs[toSignUp])
                toSignUp = scheduleForSignup();
                if(toSignUp != -1) {
                    signUpDay = signUpALib(toSignUp)

                }
            }

        } else {
            signUpDay--;
        }

        scanLibs();
        updateLibrary(D - i + 1);
    }

    val out = pathname.split("/");
    val outname = out[out.lastIndex]

    val res = ArrayList<Lib>();
    for(i in libOrder) {
        val lib: Lib = libs[i];
        if(lib.sent.size != 0) {
            res.add(lib);
        }
    }
    File("$pathname.out").printWriter().use { out ->
        out.println(res.size)
        for(lib in res) {
            out.println("${lib.id} ${lib.sent.size}")
            out.println(lib.sent.joinToString(" "))
        }

    }

    println("done executing $pathname");
}

fun scanLibs() {
    val scannedToday = HashSet<Int>();
    for(lib in scheduledLib) {
        val limit = lib.M;
        var libScanned = 0;
        while(libScanned < limit) {
            if(lib.books.size == 0) {
                scheduledLib.remove(lib);
                break;
            }
            val iter = lib.books.iterator();
            while(iter.hasNext() && libScanned < lib.M) {
                val book = iter.next();
                if(scanned.contains(book.id)) {
                    iter.remove();
                    continue;
                } else {
                    println("scanned book ${book.id}")
                    lib.sent.add(book.id);
                    scannedToday.add(book.id);
                    scanned.add(book.id)
                    libScanned++;
                    iter.remove();
                }
            }
        }
    }
}


fun scheduleForSignup(): Int {
    var maxScore = Double.MIN_VALUE;
    var libIndex = -1;
    var secondaryIndex = -1;
    for(i in 0 until libs.size) {
        val lib = libs[i];
        if(lib.scheduled || lib.T >= D) {
            continue;
        }

        var thisScore = lib.sumScore / lib.T;
        if(thisScore > maxScore) {
            if(D - lib.T <= 10) {
                secondaryIndex = i;
            }

            maxScore = thisScore.toDouble();
            libIndex = i;
        }
    }

//    if(secondaryIndex != -1) {
//        return secondaryIndex;
//    }
    return libIndex;
}

fun signUpALib(libIndex: Int): Int { //day
    val lib = libs[libIndex];
    lib.scheduled = true;
    libOrder.add(lib.id);
    return lib.T;
}

fun updateLibrary(day: Int) {
    for(lib in libs) {
        val iter = lib.books.iterator();
        while(iter.hasNext()) {
            val book = iter.next();
            if(scanned.contains(book.id)) {
                lib.N -= 1;
                lib.sumScore -= book.score;
                iter.remove();
            }
        }
        lib.sumScore = 0;
        var effectiveDays = (day - lib.T) * lib.M
        val bookIter = lib.books.iterator();
        while(bookIter.hasNext() && effectiveDays > 0) {
            val book = bookIter.next();
            lib.sumScore += book.score
            effectiveDays--;
        }
    }
}


class Lib(
        val id: Int,
        var N: Int, //numBooks
        val T: Int, //daySignup
        val M: Int, //ship/ day
        val books: ConcurrentSkipListSet<Book> = ConcurrentSkipListSet<Book>(compareBy(Book::score).thenComparingInt(Book::id).reversed())
) {
    var scheduled = false;
    var sumScore = 0;
    val sent = LinkedList<Int>();
}

class Book(
        var id: Int,
        var score: Int
) {

}
