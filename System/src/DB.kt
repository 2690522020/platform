import io.vertx.core.Future
import io.vertx.core.Future.future
import io.vertx.core.json.JsonArray
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.SQLClient
import io.vertx.ext.sql.SQLConnection
import org.joda.time.DateTime

/**
 * Created by zjw on 2017/11/7.
 */

fun SQLClient.withConnection(res: (SQLConnection) -> Future<Unit>): Future<Unit> {
    val finished = future<Unit>()
    getConnection {
        if (it.succeeded()) {
            val connection = it.result()
            val done = res(connection)
            done.setHandler {
                connection.close()
                if (it.succeeded()) finished.complete()
                else finished.fail(it.cause())
            }
        } else {
            finished.fail(it.cause())
        }
    }
    return finished
}

fun SQLClient.update(query: String, params: JsonArray): Future<Unit> {
    return withConnection {
        val finished = future<Unit>()
        it.updateWithParams(query, params, {
            if (it.succeeded()) {
                finished.complete()
            } else {
                println("ERROR: $query ($params)")
                it.cause().printStackTrace()
                finished.fail(it.cause())
            }
        })
        finished
    }
}

fun <T> SQLClient.queryOne(query: String, params:JsonArray, rsHandler: (ResultSet) -> T): Future<T> {
    val future = future<T>()
    withConnection {
        val finished = future<Unit>()
        it.queryWithParams(query, params, {
            if (it.succeeded()) {
                try {
                    val result = rsHandler(it.result())
                    future.complete(result)
                    close()
                } catch (t: Throwable) {
                    future.fail(t)
                } finally {
                    finished.complete()
                }
            } else {
                it.cause().printStackTrace()
                finished.fail(it.cause())
            }
        })
        finished
    }
    return future
}

fun <T> SQLClient.query(query: String, params: JsonArray, rsHandler: (ResultSet) -> List<T>): Future<List<T>> {
    val future = future<List<T>>()
    withConnection {
        val finished = future<Unit>()
        it.queryWithParams(query, params, {
            if (it.succeeded()) {
                try {
                    val result = rsHandler(it.result())
                    future.complete(result)
                } catch (t: Throwable) {
                    future.fail(t)
                } finally {
                    finished.complete()
                }
            } else {
                finished.fail(it.cause())
            }
        })
        finished
    }
    return future
}

fun SQLClient.execute(query: String): Future<Unit> {
    return withConnection {
        val finished = future<Unit>()
        it.execute(query, {
            if (it.succeeded()) {
                print("${DateTime.now()} execute finished :${it.succeeded()}")
                finished.complete()
            } else {
                println("ERROR: " + query)
                it.cause().printStackTrace()
                finished.fail(it.cause())
            }
        })
        finished
    }
}



