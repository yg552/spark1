import java.sql.{Connection, PreparedStatement, ResultSet}

import com.alibaba.druid.pool.DruidDataSourceFactory
import java.util.Properties

import javax.sql.DataSource

/**
 * author 杨广
 * 使用Druid连接Mysql
 * 远程连接： mysql -p ip -P port -u username -p password
 * 1.需要把用户的host改为%
 * 2.需要把mysql的bind-address 改为0.0.0.0
 */
object ConMysqlByDruid {
  def main(args: Array[String]): Unit = {
    //通过druid获取信息源
    val property = new Properties()
    property.put("driver", "com.mysql.cj.jdbc.Driver")
    property.put("url", "jdbc:mysql://localhost:3306/hah?serverTimezone=UTC")
    property.put("username", "root")
    property.put("password", "123456")

    val source: DataSource = DruidDataSourceFactory.createDataSource(property)
    val connection: Connection = source.getConnection()
    val statement: PreparedStatement = connection.prepareStatement("select _id,?,age from dome1")
    statement.setString(1, "name")
    val set: ResultSet = statement.executeQuery()
    while (set.next()) {
      println(set.getInt(1) + " " + set.getString(2) + " " + set.getInt(3))
    }
    set.close()
    statement.close()
    connection.close()
  }
}
