package com.yunkeji.hbase
import com.yunkeji.utils._
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

import java.text.SimpleDateFormat
import java.util

object HbaseTest {
  def main(args: Array[String]): Unit = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println(df.format(System.currentTimeMillis()))

    val connection: Connection = HbaseUtils.getConnection("hadoop101,hadoop102,hadoop103", 2181)
    //val connection: Connection = HbaseUtil.getConnection("hadoop101", 2181)
    import org.apache.hadoop.hbase.client.HBaseAdmin
    //在HBase中管理、访问表需要先创建HBaseAdmin对象
    //Connection connection = ConnectionFactory.createConnection(conf);
    //HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
    //val admin = new HBaseAdmin(conf)

    /*    //TODO 1.创建表管理类
        val admin: Admin = connection.getAdmin
        //TODO 2.创建表描述类
        val tableName: TableName = TableName.valueOf("test1")//表名称
        val desc = new HTableDescriptor(tableName)
        //TODO 3.创建列族的描述类
        val family1 = new HColumnDescriptor("info1")
        //TODO 4.添加到表中
        desc.addFamily(family1)
        val family2 = new HColumnDescriptor("info2")
        desc.addFamily(family2)
        //TODO 5.创建表
        admin.createTable(desc)
        println("cg")*/
    //删除表
    /* admin.disableTable(tableName)
     admin.disableTable(tableName)
     admin.close()*/
    println(df.format(System.currentTimeMillis()))
    //向表中插入数据
    val puts = new util.ArrayList[Put](100)
    val htable: Table = connection.getTable(TableName.valueOf("test1"))
    //rowkey
    /*val put = new Put(Bytes.toBytes("003"))
  put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("unsername"), Bytes.toBytes("zhaoliu"))
   put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("age"), Bytes.toBytes("27"))
   put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("sex"), Bytes.toBytes("female"))
   put.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("other"), Bytes.toBytes("teacher"))
   htable.put(put)*/
    println(df.format(System.currentTimeMillis()))
    for (i <- 1 to 10) {
      val put = new Put(Bytes.toBytes("oid"+i))
      put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("unsername"), Bytes.toBytes("zhangsan"+i))
      put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("age"), Bytes.toBytes("18"+i))
      put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("sex"), Bytes.toBytes("female"+i))
      put.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("other"), Bytes.toBytes("teacher"+i))
      puts.add(put)
    }
    htable.put(puts)

    println(df.format(System.currentTimeMillis()))
  }


}
