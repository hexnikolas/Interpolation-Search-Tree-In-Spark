import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.TaskContext
import java.io._
import java.util._
import java.io.File
import scala.util.Random
import scala.collection.JavaConversions._

object Run{
	def main(args: Array[String]) {

        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val conf = new SparkConf().setAppName("SPIS")
        val sc = new SparkContext(conf)

        println("Starting..")

        try{

			//Create result files
			
            val fw  = new FileWriter("/home/sparklab/spis/results/preprocess.txt", true)
            val fw1 = new FileWriter("/home/sparklab/spis/results/buildtime.txt", true)
            val fw2 = new FileWriter("/home/sparklab/spis/results/searchtime.txt", true)
            val fw3 = new FileWriter("/home/sparklab/spis/results/rangetime.txt", true)

            //val fw =  new FileWriter("/home/zacharato/IST/preprocess.txt", true)
            //val fw1 = new FileWriter("/home/zacharato/IST/buildtime.txt", true)
            //val fw2 = new FileWriter("/home/zacharato/IST/searchtime.txt", true)
            //val fw3 = new FileWriter("/home/zacharato/IST/rangetime.txt", true)

            val t = System.nanoTime

            var y: Double = 0.6 				//the degree of the tree
            val broadcastY = sc.broadcast(y)

            val numberofpartitions = args(0).toInt
            val broadcastpartitions = sc.broadcast(numberofpartitions)

            val rangequeries = args(1).toInt

            //val a = sc.textFile("/home/zacharato/IST/rng/test.txt").map(_.toDouble).sortBy[Double](x=>x, true, numberofpartitions)				// Should be some file on your system
            //val a = sc.textFile("/home/zacharato/IST/rng/numbers4.txt" ).flatMap(line => line.split(" +")).map(_.toInt).sortBy[Int](x=>x, true , numberofpartitions)

            val a = sc.textFile("hdfs://master2-bigdata/user/sparklab/spis/test.txt" ).flatMap(line => line.split(" +")).map(_.toDouble).sortBy[Double](x=>x, true , numberofpartitions)
            //val a = sc.textFile("hdfs://master2-bigdata/user/sparklab/topkdom/1bil2dMixture/data.txt").map(line => line.split(" +")).map{ case Array(a,b)=>a.toDouble }.sortBy[Double](x=>x, true ,numberofpartitions)
            //val a = sc.textFile("hdfs://master2-bigdata/user/sparklab/topkdom/100mil2dMixture/data.txt").flatMap(line => line.split(" +")).map(_.toDouble).sortBy[Double](x=>x, true, numberofpartitions)
            println("Input Sorted")
            println(a.getNumPartitions + " partitions")


            //val searchArr = sc.broadcast( Seq.fill(searchqueries)("%.12f".format(Random.nextDouble)).map(_.toDouble))
            val rangeArr = sc.broadcast ( Seq.fill(2*rangequeries)("%.12f".format(Random.nextDouble)).map(_.toDouble))
            //val rangeArr = sc.broadcast (Array(-30468073 , 12841, 128695 , 907713, 17571959, -5235789 , 1204812, -75453751))

            println(rangequeries+" Range Queries broadcasted")



            //fw.write( ((System.nanoTime - t) / 1e9d).toString+'\n')    //preprocess time           
			
            val t1 = System.nanoTime
            
            println("Building IST..")
            
            val d = a.mapPartitions{ iterator=>
                //println(mylist.size + " partition "+ TaskContext.get.partitionId )

                val t = System.nanoTime
                val graphA = new Graph

                val leaves = scala.collection.mutable.ArrayBuffer.empty[graphA.LeafNode]
                val inners = scala.collection.mutable.ListBuffer.empty[graphA.InnerNode]
                //val mylist = iterator.toList
                //println(mylist.size)

                
                val ROOT = graphA.CreateIST(broadcastY.value, leaves, inners , iterator)

                //println(leaves.head.storedvalue +" "+ leaves.last.storedvalue)
                //println(leaves.size + inners.size)
                val buf = scala.collection.mutable.ListBuffer.empty[graphA.Node]
                buf+=ROOT
                buf++=inners
                buf++=leaves
               
                buf.toIterator

            }
            
            //val totalnodes= d.count
            //println("done, "+ totalnodes + " nodes")
            //println(d.count())
            d.cache()
            
            
            fw1.write( ((System.nanoTime - t1) / 1e9d).toString+'\n')       //build time
        /*    
            val t2 = System.nanoTime
        
            println("Searching..")
            val queries = d.mapPartitions{iterator=>
                var mylist = iterator.toList
                val graphA = new Graph
                var root = mylist.head  
                //var root = iterator.next()
                val searched = scala.collection.mutable.ArrayBuffer.empty[Double]


                //println(root.asInstanceOf[graphA.RootNode].low ,root.asInstanceOf[graphA.RootNode].high )

                searchArr.value.foreach{j=>
                    if(( j > root.asInstanceOf[graphA.RootNode].high ) || ( j < root.asInstanceOf[graphA.RootNode].low )){
                        //println("not here")                        
                    }else{
                        //println("asdsada")
                        //println(j)
                        var noot = graphA.Search(root.asInstanceOf[graphA.InnerNode]  , j)
                        if(noot != null){

                            if (noot.asInstanceOf[graphA.LeafNode].storedvalue == j){
                                //println(noot.asInstanceOf[graphA.LeafNode].position)
                                //println(j +" found at partition "+ TaskContext.get.partitionId) 
                                searched.append(j)
                            }
                        }
                        noot = null
                    }
                }
                //println(searched.size)
                searched.toIterator
            }            
            println(queries.count)
            print(" done")
            println()
            fw2.write( ((System.nanoTime - t2) / 1e9d).toString+'\n')       //search time

            */
            val t3 = System.nanoTime
            
            println("Range Searching..")
            val rangeq = d.mapPartitions{iterator =>
                var results = scala.collection.mutable.ArrayBuffer.empty[(Int,Int)]
                var counter = 0;
                var mylist = iterator.toList
                val graphA = new Graph
                var root = mylist.head 

                for ( i <- 0 to rangeArr.value.size-1 by 2){
                    if( rangeArr.value(i) < rangeArr.value(i+1)){
                        var rangelements = graphA.RangeSearch(root.asInstanceOf[graphA.RootNode] , rangeArr.value(i), rangeArr.value(i+1))
                        results.append((counter,rangelements))
                        //println(rangeArr.value(i) + " " + rangeArr.value(i+1) + " " +pame)
                    } else {
                        var rangelements = graphA.RangeSearch(root.asInstanceOf[graphA.RootNode] , rangeArr.value(i+1 ), rangeArr.value(i))
                        results.append((counter,rangelements))
                        //println(rangeArr.value(i+1) + " " + rangeArr.value(i) + " " +pame)
                    }
                    counter+=1;
                }
                println(results)
                results.toIterator
            }
            val telos = rangeq.reduceByKey(_+_).count
            //val telos = rangeq.reduceByKey(_+_).collect().foreach(println)
                        
            fw3.write( ((System.nanoTime - t3) / 1e9d).toString+'\n')       //range time
			
            fw.close()
            fw1.close()
            fw2.close()
            fw3.close()
    
        }
        catch {
          case ex: ArrayIndexOutOfBoundsException => {
            println("Not enough arguments")
          }
          case ex: NumberFormatException => {
            println("Needs integer")
          }
          case ex: org.apache.hadoop.mapred.InvalidInputException => {
            println("Missing file")
          }
        }
        finally{   
                
            println("Complete")
            sc.stop()
        }
    }
}