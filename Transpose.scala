 //export HADOOP_CONF_DIR=/etc/hadoop/conf.cloudera.yarn
 // spark-submit --deploy-mode cluster  --num-executors 10 --driver-memory 2g  --jars hdfs://twnva06.twn.sas.com:8020/tmp/jedis-2.1.0.jar --master yarn --class com.sas.tsmc.Transpose ./target/scala-2.10/tsmchpdm_2.10-1.0.jar /tmp/trdata /tmp/tr1 1 2,3 4 twnva06 /tmp/sasmeta
  package com.sas.tsmc
  import org.apache.spark.SparkContext._
  import org.apache.spark.rdd._
  import org.apache.spark._      
  import org.apache.hadoop.fs.{FileUtil, Path, FileSystem} 
  import java.net.URI
  import org.apache.hadoop.conf.Configuration
  import scala.collection.JavaConverters._
  import java.util.Properties
  import org.apache.log4j.{Logger,Level}
  import java.io.{BufferedWriter, OutputStreamWriter, FileInputStream, File}
  import scala.collection._
  import org.apache.spark.util._

  
  object Transpose {
     val log =  Logger.getLogger("com.sas.tsmc")
 
     val srcDelimeter=","
  
  def transpose(in : Array[String], byColPos : Array[Int] , byColsPosMap : Map[String,String],valColPos : Int) : String = {
    //val byCol = in(byColPos)    //single byCol
    val byCol = getByColsName(in,byColPos) // multiple byCols and concate as key
    // no need since it is split(,-1)
    
    if (in.size-1 < valColPos)  
        " "+"="+byColsPosMap(byCol).toString   //due to missing value
    else
        in(valColPos)+"="+byColsPosMap(byCol)    //.toString
  }
  
 
  def getByColsName(x : Array[String], byColPos : Array[Int]) : String = { 
     var res =""
     var i =0 
     while (i < byColPos.size) {
          res = res + x(byColPos(i))
          i = i + 1
     }
     res
     /**
     byColPos.foldLeft(new StringBuilder())( (y,colpos) => { 
                            y.size match { 
                            case 0 => y append x(colpos)
                            case _ => y append "_" append x(colpos)
                            }
                  }).toString
     */               
  }
  
  def outputSchema(sc : SparkContext, outputDir: String,metaDir : String, schema1 : String ,byColPos : Array[Int] ,valColPos : Int ,byColsPosMap :Map[String,Int],keyPos :List[Int],keepNonTrCols : Boolean,prefixName : String,transposeColSchema: Map[String,String]) = {
      val uritmpdir =  new URI("/tmp")
      val hdfs: FileSystem = FileSystem.get(uritmpdir,sc.hadoopConfiguration)
      val filename =  outputDir.substring(outputDir.lastIndexOf("/")+1)
      val parafile = "p_"+ filename
      Query.hdfswrite(hdfs,metaDir+"/"+filename+"/"+parafile+".csv",byColsPosMap.map(x  => prefixName+(x._2+1)+","+x._1).toList); 
      Query.outputVarCodeHdmd(hdfs, metaDir+"/"+filename+"/"+parafile+".csv", outputDir, metaDir,filename+".sashdmd");
      val meta2List =  byColsPosMap.map(x => prefixName+ (x._2+1)+"="+transposeColSchema.getOrElse(x._1,"double")).toList
      //val meta2List = transposeColSchema.keySet.map( x => prefixName+(byColsPosMap.getOrElse(x,0)+1)+"="+transposeColSchema.getOrElse(x,"double")).toList
      val dropbyCol_byVal =  byColPos :+ valColPos  
      val meta1 =  schema1.split("~\\|") 
      val meta1List =  if (keepNonTrCols) JoinData.remove(meta1,dropbyCol_byVal.sorted.toList).toList  else { for (i <- 0 to keyPos.size-1) yield (meta1(keyPos(i))) }
      //val meta1 =  schema1.split("~\\|")
      //val meta1List = for (i <- 0 to keyPos.size-1) yield (meta1(keyPos(i)))
      
      //Query.genMetaData(hdfs,outputDir, metaDir, filename+".sashdmd", meta1List.toList,meta2List)
      val allschema = meta1List.toList ++ meta2List
      Query.genMetaData(hdfs,outputDir, metaDir, filename+".sashdmd",allschema,List[String]())
  }
  def mkouputString( k : String , v : String , size : Integer) = {
        val datastr = Array.fill(size )("")
        val data = v.split(",")
        var i = 0;
        while (i < data.size) {
              val val_pos = data(i).split("=")
              datastr(val_pos(1).toInt)= val_pos(0) //it stored val=pos pos start from 0 need to add fixedColCnt                           
              i = i+1
            }
        k+","+datastr.mkString(",")
  }
  
   def mkouputStringNoKey(v : String , size : Integer) = {
        val datastr = Array.fill(size )("")
        val data = v.split(",")
        var i = 0;
        while (i < data.size) {
              val val_pos = data(i).split("=")
              datastr(val_pos(1).toInt)= val_pos(0) //it stored val=pos pos start from 0 need to add fixedColCnt                           
              i = i+1
            }
        datastr.mkString(",")
  }
  
  def fixColpart(x : Array[String],keyPos:List[Int],byColPos: Array[Int],valColPos:Int ) = {
          val keydata =for (i <- 0 to keyPos.size-1) yield (x(keyPos(i)))
          val removeindex = byColPos :+ valColPos
          val finalremove = removeindex.distinct.sortWith(_ < _).toList
          val valdata =  JoinData.remove(x,finalremove)
          (keydata.toArray.mkString(","), valdata.mkString(",")) 
  }
  def judgeType(colVal : String) = {
       //return (type,len) due to char varchar need len
       val Date = """(\d\d\d\d)-(\d\d)-(\d\d)""".r
       val DateTime = """(\d\d\d\d)-(\d*\d)-(\d*\d) (\d\d):(\d\d):(\d\d)""".r
       var result =("",0)
       val dateformat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
       try {
          val tmp =colVal.toDouble
          result = ("DOUBLE",0)
       } catch  {
          case e:Exception =>  {
              if (Date.findFirstMatchIn(colVal) != None) 
                  result = ("DATE",0)
              else if (DateTime.findFirstMatchIn(colVal) !=None )
                  result = ("TIMESTAMP",0)
              else
                  result = ("VARCHAR",colVal.size)
          }
      }
      result
  }
  def replaceLen(colName:String,typeLen:(String,Int),schemaLenMap:scala.collection.mutable.Map[String,Int] ) = {
     if (schemaLenMap.contains(colName) && schemaLenMap.getOrElse(colName,999999) < typeLen._2)
          schemaLenMap(colName)=typeLen._2
     else
         schemaLenMap(colName)=typeLen._2  
  }
  def processType( sample : Array[(String, String)], byColsPosMap:Map[String,String]) = {
         val schemaLenMap: scala.collection.mutable.Map[String,Int] =  scala.collection.mutable.Map() //for the char type length usage
         val schemaMap: scala.collection.mutable.Map[String,String] =  scala.collection.mutable.Map() //for the char type length usage
         byColsPosMap.keySet.foreach(x => schemaMap(x)="" )  //initail col type to empty string and for keep the colPos
         val revsersePosMap =  byColsPosMap.map({case (a,b) => (b -> a)})
         sample.foreach( in => 
              { val x = in._2                
                val valColPos = x.split("=")
                val colName = revsersePosMap.getOrElse(valColPos(1),"")
                val typeLen = judgeType(valColPos(0))
                if (typeLen._1 !="" || typeLen._1 != null) {  //if empty keep on next sample row
                  typeLen._1 match {
                    case "CHAR"  =>  schemaMap(colName)="char"; replaceLen(colName,typeLen,schemaLenMap) 
                    case "VARCHAR" => schemaMap(colName)="varchar"; replaceLen(colName,typeLen,schemaLenMap) 
                    case "TIMESTAMP" => schemaMap(colName)="timestamp"
                    case "DATE" => schemaMap(colName)="date"
                    case "DOUBLE" => schemaMap(colName)="double"
                    case _ => schemaMap(colName)="double"
                  } 
                }                             
              }
            )
        schemaLenMap.keySet.foreach(x => schemaMap(x)= schemaMap(x)+"("+schemaLenMap(x)+")")    
       schemaMap
  }
  def getTrasposeSchema(groupByKeyRdd:RDD[(String,String)],byColsPosMap:Map[String,String])  = {      
     
      val sample = groupByKeyRdd.take(3) //sample rows maybe larger 100 or 50
      val colValPos =  processType(sample,byColsPosMap)            
      colValPos       
      
  }                                                                                                     
  def main(args: Array[String]) {
   args.foreach(x => log.info(x+" "))
    println
    if (args.length != 7 && args.length !=8 )
    {
        println("Error not enough parameters")
        println("Usage : Transpose inputDir outputDir keyPos byColPos[eg 3,5] valColPos metaDir prefixName keepNonTrCols(Y/N)")
        System.exit(1)
    }      
    val inputDir = args(0)
    val outputDir = args(1)     
    val keyPos = args(2).split(",").toList.map(_.toInt) //args(2).toInt  change to multiple key -- eg 0,1
    val byColPos = args(3).split(",").map(_.toInt) // if the byCols is more than 1 eg WAT1,SITE1 it will be like 3,5
    val valColPos = args(4).toInt  
    val metaDir = args(5)  
    val prefixName = args(6)
    val keepNonTrCols = if (args.length == 8 && "Y" == args(7)) true else false 
         
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val prjname = outputDir.substring(outputDir.lastIndexOf("/")+1)
    //System.setProperty("SPARK_YARN_APP_NAME","SAS Transpose data - "+prjname)
    val conf = new SparkConf().setAppName("SAS Transpose data - "+prjname)
    conf.set("spark.core.connection.ack.wait.timeout", "1200")
    conf.set("spark.akka.frameSize", "50")
     conf.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer")   //.set("spark.kryoserializer.buffer.mb","24") 
    val notsashdmd = inputDir.indexOf(".sashdmd") < 0  && inputDir.indexOf(".hive") < 0  //hive process like sashdmd
    val tbegin = System.currentTimeMillis
    val sc = new SparkContext(conf)
     val fs = FileSystem.get(sc.hadoopConfiguration)
     val o_path=  new Path(outputDir)
     if (fs.exists(o_path))
     {
         println("outputDir "+outputDir+" exists will remove it")
         fs.delete(o_path,true)
     }
    val tuple = if (!notsashdmd) JoinData.getInputDirSepSchema(sc,inputDir) else ("","",Array(""),0)
    val schema = if (notsashdmd) JoinData.loadSchema(inputDir, sc.hadoopConfiguration) else Map("" -> "")
    val schema1 = if (notsashdmd)  schema("schema1") else tuple._3.mkString("~|")
    val schema2 = if (notsashdmd)  schema("schema2") else ""
    val delimeter = if (notsashdmd) schema.getOrElse("delimeter",srcDelimeter) else tuple._2
    val realInputDir = if (notsashdmd)  inputDir else tuple._1
    val rdd =  sc.textFile(realInputDir)
     val schemaLen =  if (schema2 != "") schema1.split("~\\|").size+schema2.split("~\\|").size  else  schema1.split("~\\|").size
    val hasExtraDelimeter = JoinData.checkExtraDelimeter(delimeter,rdd.take(3),schemaLen)
    //log.info("schema len "+schemaLen)
    //log.info("has extra delimeter "+hasExtraDelimeter)
    val inRdd = if (hasExtraDelimeter) rdd.map(_.split(delimeter))  else rdd.map(_.split(delimeter,-1))   //sashdmd always has delimeter \u0001 in the last   
    //inRdd.cache
    val distinctbyCols = inRdd.map(x => getByColsName(x,byColPos) ).distinct.collect.sorted //sort so it same get together
    //val distinctbyCols = inRdd.map(_(byColPos)).distinct.collect
    val byColsPosMap = distinctbyCols.foldLeft(scala.collection.mutable.LinkedHashMap[String,Int]())((b,a) => {  b += (a -> b.size ) ;  b })
    val byColsPosMap2 =  byColsPosMap.map( { case (k,v) => (k,v.toString) } )  //traslate to String so no need in RDD call to String
    //inRdd.map(x => { outputRdd(x(keyPos)) = outputRdd(x(keyPos)) ++ transpose(x,byColPos,byColsPosMap,valColPos) })
      /*
      val transposeRdd = inRdd.mapPartitions{ iter => 
         iter.map( x => (JoinData.makeKey(x,keyPos),transpose(x,byColPos,byColsPosMap2,valColPos)))
      } */
      val transposeRdd = inRdd.map( x => (JoinData.makeKey(x,keyPos),transpose(x,byColPos,byColsPosMap2,valColPos)))
      val totalColsCnt = infoRdd.take(1)(0).size
      val fixedColCnt =  totalColsCnt- byColPos.size -1 //plus valCol and byCols --multiple
      val byColsCnt = distinctbyCols.size 
      val groupByKeyRdd =  transposeRdd.reduceByKey((a,b) => (a+","+b))
      val transposeColSchema = getTrasposeSchema(groupByKeyRdd,byColsPosMap2)
      if (fixedColCnt == 0 || !keepNonTrCols)  {
          groupByKeyRdd.map(x => mkouputString(x._1,x._2,byColsCnt)).saveAsTextFile(outputDir)
         } 
      else {    
        val fixColRdd = inRdd.map( x => fixColpart(x,keyPos,byColPos,valColPos)).distinct
        //shoud not ouput key since FixColRdd already has it
        fixColRdd.join(groupByKeyRdd).map( x => mkouputString(x._2._1,x._2._2,byColsCnt) ).saveAsTextFile(outputDir) 
      }
    
     outputSchema(sc,outputDir,metaDir,schema1,byColPos,valColPos,byColsPosMap,keyPos,keepNonTrCols,prefixName,transposeColSchema)
      
    val tend = System.currentTimeMillis
   
    log.info(" total run time "+ (tend-tbegin)/1000 +" seconds") 
    println(" total run time "+ (tend-tbegin)/1000 +" seconds")
    
  }
  
  }
  