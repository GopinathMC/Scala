import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.types._

object processIBPFiles {
  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder.appName("SSNA").getOrCreate()
    val conf = new SparkConf()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext 
  
  
  def getStatus(mrp:String,status:String,Flag:String) : String = {
    if(mrp != "X0") {return s"MRP type should be changed from ${mrp} to X0 in MARC table"}
    else if(status == "Z5") {return s"Material status should not be z5"}
    else if(Flag == "X") {return s"Please remove the deletion flag(X)"}
    else if(mrp != "X0" && status == "Z5") {return s"MRP type should be changed from ${mrp} to X0 and status should not be z5"}
    else if(mrp != "X0" && Flag == "X") {return s"MRP type should be changed from ${mrp} to X0 and deletion flag(X) should be removed"}
    else if(status == "Z5" && Flag == "X") {return s"Material status should not be z5 and remove the deletion flag(x)"}
    else if(mrp != "X0" && status == "Z5" && Flag == "X") {return s"MRP type should be changed from ${mrp} to X0 and Material status should not be z5 and remove the deletion flag(x)"}
    else {return "Some other filters"}
    
  }
    val getStat = spark.udf.register("getStat",getStatus(_:String,_:String,_:String) : String)
  def processFile(filePath:String):Unit = {
    val query1 = "select locid,prdid from SalesOrdTbl where concat(locid,prdid) not in (select concat(locid,prdid) from LocProdTbl)"
    val query2 = "select t2.* from (select distinct TRIM(MARC.WERKS ) as LOCID,TRIM(regexp_replace(MARC.MATNR ,'^0+','') ) as PRDID,MARC.DISMM AS MRPType,CAST(MARC.MMSTA as string) AS Material_Status,MARC.LVORM AS Deletion_Flag from MARC)t2"
    val locPrdActiveDF = spark.read.format("csv").option("header","true").option("inferSchema","true").load(filePath)
    //locPrdActive.show()
    val salesOrderDF = spark.read.table("prd_product_fibi_ibp.td_opensalesorder")
    locPrdActiveDF.createOrReplaceTempView("LocProdTbl")
    salesOrderDF.createOrReplaceTempView("SalesOrdTbl")
    val locProdMissDF = spark.sql(query1)
    val marc_a = spark.read.table("prd_product_fibi_ibp.marc_a")
    marc_a.createOrReplaceTempView("MARC")
    val marcDF = spark.sql(query2)
    val salesOrderRejectDF = salesOrderDF.join(locProdMissDF,Seq("locid","prdid"),"inner").join(marcDF,Seq("locid","prdid"),"inner")
    val finalDF = salesOrderRejectDF.withColumn("Corrective_Measure",getStat(col("MRPType"),col("Material_Status"),col("Deletion_Flag"))).select(col("locid")) 
    //val locProdMissStatusDF = locProdMissDF.map(getStatus(locProdMissDF.col("locid"),locProdMissDF.col("prdid")))
    finalDF.write.option("header","true").option("Sep",",").format("csv").mode("overwrite").save("")
  }
  processFile("/FileStore/tables/NA_CSST_MD_LOCATIONPRODUCT_INACTIVE.csv")
}
}
