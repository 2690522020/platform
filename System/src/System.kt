import io.vertx.core.Future
import io.vertx.core.Vertx.vertx
import io.vertx.core.VertxOptions
import io.vertx.core.json.JsonObject
import io.vertx.ext.asyncsql.AsyncSQLClient
import io.vertx.ext.asyncsql.PostgreSQLClient
import io.vertx.kotlin.core.json.JsonArray
import io.vertx.kotlin.core.json.array
import io.vertx.kotlin.core.json.json
import org.joda.time.DateTime

/**
 * Created by zjw on 2017/11/7.
 */

object System {
    @JvmStatic
    fun main(args: Array<String>) {
        val userService = UserServiceDB(postgresClient)
        vertex.restApi(9000) {

            get("/user/all") { send(userService.getAllUser()) }

            get("/user/:userId") { send(userService.getUser(param("userId"))) }

            post("/user") { send(userService.addUser(bodyAs(User::class))) }

            put("/user") { send(userService.updateUser(bodyAs(User::class))) }

            delete("/user/:userId") { send(userService.remUser(param("userId"))) }

        }
    }
}

/**---------------------------------------------------Class----------------------------------------------------------**/
data class User(val Id: Int,
                val KeyId: String?,
                val UserName: String,
                val Password: String,
                val EMail: String?,
                val Name: String?,
                val IsActive: Boolean,
                val LastVisit: DateTime?,
                val Creator: Int?,
                val CreatedateTime: DateTime?,
                val Updater: Int?,
                val UpdateTime: DateTime?)

/**---------------------------------------------------Basics---------------------------------------------------------**/
val option = VertxOptions()
        .setEventLoopPoolSize(4)
        .setWorkerPoolSize(80)
        .setClustered(false)!!

val vertex = vertx(option)!!

val postgresClientConfig = JsonObject()
        .put("host", "localhost")
        .put("port", 5432)
        .put("username", "postgres")
        .put("password", "Passw0rd")
        .put("database", "silverback")!!

val postgresClient = PostgreSQLClient.createShared(vertex, postgresClientConfig)!!

/**---------------------------------------------------Services-------------------------------------------------------**/

interface UserService {

    fun getUser(id: String): Future<User?>
    fun addUser(user: User): Future<Unit>
    fun remUser(id: String): Future<Unit>
    fun updateUser(user: User): Future<Unit>

    fun getAllUser(): Future<List<User>>
}

class UserServiceDB(private val client: AsyncSQLClient) : UserService {

    override fun addUser(user: User): Future<Unit> =
            client.update(
                    "INSERT INTO T0001 (KEYID,USERNAME, PASSWORD, EMAIL,NAME,ISACTIVE,LASTVISIT,CREATOR,CREATETIME,UPDATER,UPDATETIME ) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    JsonArray(user.KeyId, user.UserName, user.Password, user.EMail, user.Name, user.IsActive, user.LastVisit, user.Creator, user.CreatedateTime, user.Updater, user.UpdateTime))

    override fun getUser(id: String): Future<User?> =
            client.queryOne("SELECT ID,KEYID,USERNAME, PASSWORD, EMAIL,NAME,ISACTIVE,LASTVISIT,CREATOR,CREATETIME,UPDATER,UPDATETIME FROM T0001 WHERE ID=?", json { array(id) }) {
                it.results.map {
                    User(it.getInteger(0), it.getString(1), it.getString(2), it.getString(3), it.getString(4), it.getString(5)
                            , it.getBoolean(6), it.getValue(7) as? DateTime, it.getInteger(8), it.getValue(9)as? DateTime, it.getInteger(10), it.getValue(11)as? DateTime)
                }.first()
            }

    override fun updateUser(user: User): Future<Unit> =
            client.update(
                    "UPDATE T0001 SET KEYID=?,USERNAME=?, PASSWORD=?, EMAIL=?,NAME=?,ISACTIVE=?,LASTVISIT=?,CREATOR=?,CREATETIME=?,UPDATER=?,UPDATETIME=? WHERE ID = ? ",
                    json { array(user.KeyId, user.UserName, user.Password, user.EMail, user.Name, user.IsActive, user.LastVisit, user.Creator, user.CreatedateTime, user.Updater, user.UpdateTime, user.Id) }
            )

    override fun remUser(id: String): Future<Unit> =
            client.update("DELETE FROM T0001 WHERE ID = ?", json { array(id) })

    override fun getAllUser(): Future<List<User>> =
            client.query(
                    "SELECT ID,KEYID,USERNAME, PASSWORD, EMAIL,NAME,ISACTIVE,LASTVISIT,CREATOR,CREATETIME,UPDATER,UPDATETIME FROM T0001 ", json { array() }) {
                it.results.map {
                    User(it.getInteger(0), it.getString(1), it.getString(2), it.getString(3), it.getString(4), it.getString(5)
                            , it.getBoolean(6), it.getValue(7) as? DateTime, it.getInteger(8), it.getValue(9)as? DateTime, it.getInteger(10), it.getValue(11)as? DateTime)
                }.sortedBy { it.Id }
            }
}


