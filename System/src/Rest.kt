import com.google.gson.Gson
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.http.HttpServer
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import kotlin.reflect.KClass

/**
 * Created by zjw on 2017/11/7.
 */

val gson = Gson()

fun Vertx.restApi(port: Int, body: Router.() -> Unit) {
    createHttpServer().restApi(this, body).listen(port) {
        if (it.succeeded()) println("Server listening at $port")
        else println(it.cause())
    }
}

fun HttpServer.restApi(vertx: Vertx, body: Router.() -> Unit): HttpServer {
    val router = Router.router(vertx).apply {
        route().handler(BodyHandler.create())
        body()
    }
    requestHandler { router.accept(it) }
    return this
}

fun Router.get(path: String, rctx:RoutingContext.() -> Unit) = get(path).handler { it.rctx() }!!
fun Router.post(path: String, rctx:RoutingContext.() -> Unit) = post(path).handler { it.rctx() }!!
fun Router.put(path: String, rctx:RoutingContext.() -> Unit) = put(path).handler { it.rctx() }!!
fun Router.delete(path: String, rctx:RoutingContext.() -> Unit) = delete(path).handler { it.rctx() }!!

fun RoutingContext.param(name: String): String =
        request().getParam(name)

fun <T> RoutingContext.bodyAs(clazz: KClass<out Any>): T {
    return gson.fromJson(bodyAsString, clazz.java) as T
}

fun <T : Any?> RoutingContext.send(future: Future<T>) {
    future.setHandler {
        if (it.succeeded()) {
            val res = if (it.result() == null) "" else gson.toJson(it.result())
            response().headers().set("content-type", "application/json")
            response().end(res)
        } else {
            response().setStatusCode(500).end(it.cause().toString())
        }
    }
}