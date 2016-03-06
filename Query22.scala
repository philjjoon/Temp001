package de.tub.cs.bigbench

import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._
import org.apache.flink.api.common.operators.Order

/*
Developed By Philip Lee

Configuration
"/home/jjoon/bigBench/data-generator/output/item.dat" "/home/jjoon/bigBench/data-generator/output/date_dim.dat"  "/home/jjoon/bigBench/data-generator/output/inventory.dat" "/home/jjoon/bigBench/data-generator/output/warehouse.dat" "/home/jjoon/bigBench/results/"
*/

object Query22{

  // arg_configuration
  val DATE= "2001-05-08"
  val price_min1=0.98
  val price_max1=1.5
  val DAY = 24 * 60 * 60 * 1000


  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }


    val dateFormat = new _root_.java.text.SimpleDateFormat("yyyy-MM-dd")
    val dateConf = dateFormat.parse(DATE).getTime

    val env = ExecutionEnvironment.getExecutionEnvironment

    val wareHouse = getWarehouseDataSet(env)
    val filterDateDim = getDateDimDataSet(env).filter(items => Math.abs(dateFormat.parse(items._date).getTime - dateConf)/DAY <= 30)

    val invJoinWithOthers = getInventoryDataSet(env)
      .joinWithTiny(filterDateDim).where(_._date_sk).equalTo(_._date_sk)
      .joinWithTiny(wareHouse).where(_._1._warehouse_sk).equalTo(_._warehouse_sk).apply((indd,wh) => (indd._1,indd._2,wh))

    val itemJoinWithOthers = getItemDataSet(env).filter(items => items._current_price >= price_min1 && items._current_price <= price_max1)
      .joinWithHuge(invJoinWithOthers).where(_._item_sk).equalTo(_._1._item_sk).apply((item,others) => (others._3._warehouse_name,item._item_id,others._2._date,others._1._quantity_on))
      .groupBy(0,1)
      .reduceGroup(new DiffDate)
      .filter(items => items._3 > 0 && items._4/items._3 >= 2.0/3.0 && items._4/items._3 <= 3.0/2.0)
      .sortPartition(0,Order.ASCENDING).setParallelism(1)
      .sortPartition(1,Order.ASCENDING).setParallelism(1)   // FLINK 0.10 fix this issue
      .first(100)

//    itemJoinWithOthers.print()
    itemJoinWithOthers.writeAsCsv(outputPath,"\n", ",",WriteMode.OVERWRITE)

    env.execute("Big Bench Query22 Test")
  }

  // out: Collector[warehouse_name, item_id, inv_before, inv_after]
  class DiffDate extends GroupReduceFunction[(String, String, String, Int),(String, String, Double, Double)] {
    override def reduce(in: java.lang.Iterable[(String, String, String, Int)], out: Collector[(String, String, Double, Double)]) = {


      val dateFormat = new _root_.java.text.SimpleDateFormat("yyyy-MM-dd")
      val dateConf = dateFormat.parse(DATE).getTime

      var wareHouse_name = ""
      var item_id = ""
      var date = ""
      var before_quantity1 = 0
      var after_quantity2 = 0

      in.asScala.foreach{ items =>

        wareHouse_name = items._1
        item_id = items._2
        date = items._3

        if((dateFormat.parse(date).getTime - dateConf)/DAY <0)
          before_quantity1 += items._4
        else if((dateFormat.parse(date).getTime - dateConf)/DAY >=0)
          after_quantity2 += items._4

      }

      out.collect(wareHouse_name,item_id,before_quantity1.toDouble,after_quantity2.toDouble)
    }
  }


  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************
  //Long(0), String(2)
  case class WareHouse(_warehouse_sk: Long,_warehouse_name: String)
  //Long(0) String(2)
  case class DateDim(_date_sk: Long, _date: String)
  //Long(0), Long(1), Long(2), Int(3)
  case class Inventory(_date_sk: Long, _item_sk: Long, _warehouse_sk: Long, _quantity_on: Int)
  //Long(0), String(1), Double(6)
  case class Item(_item_sk: Long, _item_id: String, _current_price: Double )

  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************

  private var wareHousePath: String = null
  private var inventoryPath: String = null
  private var dateDimPath: String = null
  private var itemPath: String = null
  private var inputPath: String = null
  private var outputPath: String = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 2) {
      inputPath = args(0)
      outputPath = args(1)
      wareHousePath = inputPath + "/warehouse/warehouse.dat"
      inventoryPath = inputPath + "/inventory/inventory.dat"
      dateDimPath = inputPath + "/date_dim/date_dim.dat"
      itemPath = inputPath + "/item/item.dat"
      true
    } else {
      System.err.println("Usage: Big Bench 2 Arguments")
      false
    }
  }

  private def getWarehouseDataSet(env: ExecutionEnvironment): DataSet[WareHouse] = {
    env.readCsvFile[WareHouse](
      wareHousePath,
      fieldDelimiter = "|",
      includedFields = Array(0, 2),
      lenient = true
    )
  }

  private def getDateDimDataSet(env: ExecutionEnvironment): DataSet[DateDim] = {
    env.readCsvFile[DateDim](
      dateDimPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 2),
      lenient = true
    )
  }

  private def getItemDataSet(env: ExecutionEnvironment): DataSet[Item] = {
    env.readCsvFile[Item](
      itemPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 1, 5),
      lenient = true
    )
  }

  private def getInventoryDataSet(env: ExecutionEnvironment): DataSet[Inventory] = {
    env.readCsvFile[Inventory](
      inventoryPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 1, 2, 3),
      lenient = true
    )
  }
}

class Query22 {

}

