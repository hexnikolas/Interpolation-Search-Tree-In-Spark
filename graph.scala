import java.io._
import java.util._
import java.io.File
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.JavaConversions._

class Graph extends Serializable {


  class Node extends Serializable{
    var parent: InnerNode = null //parent of each node    
    var storedvalue: Double = 0 
  }

  class InnerNode extends Node {
    val children = scala.collection.mutable.ArrayBuffer.empty[Node]		//array buffer of children
    var nodesbelow: Int = 0 											//number of leaves the subtree has after rebuild
    var counter: Int = 0 												//counts insertions + deletions
  }
  class LeafNode extends Node {
    var position: Int = 0
  }
  class RootNode extends InnerNode {
    var low = Double.MaxValue
    var high = Double.MinValue 
  }

  /*
   * Performs an interpolation search in the REP array of the passed 
   * node , returning the correct element
   */
  def InterpolationSearch(random: InnerNode, toFind: Double): Int = {

    if ((random.children.isEmpty) || (toFind <= random.children(0).storedvalue )) {
      //println("first")
      return 0
    } else if (toFind >= random.children.last.storedvalue) {
      //println("last")
      return (random.children.size-1)
    } else if (random.children.size < 4 ){
      //println("<5")
      var mid = 0
      while(random.children(mid).storedvalue <= toFind ){
        if(mid < (random.children.size + 1)){
          mid += 1
        }
      }
      return (mid - 1)
    } else {
      //println("else")
      var low: Int = 0
      var high: Int = random.children.size - 1
      var mid: Int = 0
      while ((random.children(low).storedvalue <= toFind) && (random.children(high).storedvalue >= toFind)) {

        if(random.children(high).storedvalue==random.children(low).storedvalue){
          return low
        }else{
          var tempdown :Double = (high-low)/(random.children(high).storedvalue-random.children(low).storedvalue)
          mid = (low + (toFind - random.children(low).storedvalue)* tempdown).toInt


        }
        if (random.children(mid).storedvalue < toFind) {
          low = mid + 1
        } else if (random.children(mid).storedvalue > toFind) {
          high = mid - 1
        } else {
          return mid
        }
      }
      if (random.children(low).storedvalue == toFind) {
        return low
      }
      while (toFind >= random.children(mid).storedvalue) {
        mid += 1
      }      
      return (mid - 1)      
    }
  }


  /*
   * Searches the tree starting from the root for a specific value
   * Returns the node with the left closest value if it doesnt find the
   * value searched for.
   */
  def Search(ROOT: InnerNode, searchvalue: Double): LeafNode = {

    var local = ROOT
    var leaf :LeafNode = null
    var k: Int = 0
    var flag = true
    //println("searched for " + searchvalue)
    while ( flag )  {
      k = InterpolationSearch(local , searchvalue)
      if(local.children(k).isInstanceOf[InnerNode]){
        local = local.children(k).asInstanceOf[InnerNode]
      }else{
        leaf = local.children(k).asInstanceOf[LeafNode]
        flag = false
      }
    }
    leaf
  }


  
  def RangeSearch(ROOT: RootNode ,  min: Double , max: Double):Int={
    var correctnodes:Int = 0
    var  here, there:Int = -1

    
    if ((min >= ROOT.low) && (min <= ROOT.high)){
      val a = Search(ROOT, min)
      if(a.storedvalue==min){
        here = a.position
      }else{
        here = a.position + 1
      }
      if (max >= ROOT.high){
        there = ROOT.nodesbelow-1
      }else{
        there = Search(ROOT, max).position
      }
    }else if ((max >= ROOT.low) && (max <= ROOT.high )){
      there = Search(ROOT , max).position
      here = 0

    }else if ((min < ROOT.low) && max > ROOT.high ){
      here = 0
      there = ROOT.nodesbelow-1
    }
    if ((here != -1) && (there != -1)){
      correctnodes = there-here + 1
    }
    /*if(!correctnodes.isEmpty){
      println("leaves "+leaves.head.storedvalue + " " + leaves.last.storedvalue+ " "+ leaves.size+ " correct "+correctnodes.head.storedvalue+" "+correctnodes.last.storedvalue+" "+correctnodes.size)
    }*/
    correctnodes
  }


  /*
   * Creates a link between two nodes(child , father) by setting
   * the parent of the child equal to father, and inserting the
   * child to the children array of father in the correct position
   */
  @inline def CreateLink(parent: InnerNode, leaf: Node, position: Int) = {

    leaf.parent = parent
    //println("parent " + parent.storedvalue + " kid " + leaf.storedvalue + " position " + position)
    parent.children.insert(position, leaf)
  }


  /*
   * Deletes a link between two nodes (father , child ) 
   * by setting the parent pointer of child to null ,
   * and removing the pointer to child from children array of father
  */ 
  def DeleteLink(upper: InnerNode, child: Node) {

    if (child.parent == child) {
      upper.children -= child
    } else {
      child.parent = null
      upper.children.remove(upper.children.indexOf(child))
      if (upper.children.isEmpty) {
        DeleteLink(upper.parent.asInstanceOf[InnerNode], upper )
      }
    }
  }


  /*
   * Increases the counters on the path from the newly inserted/marked to be deleted node
   * to the root of the tree.
   * Also finds the node that violates the rebuild
   * condition to start the rebuilding process   * 
   */
  def IncreaseCounters(ROOT: InnerNode, leaf: LeafNode): InnerNode = {

    var local:Node = leaf
    var rebuildhere: InnerNode = null
    var aflag: Boolean = true

    while (aflag) {
      local = local.parent
      local.asInstanceOf[InnerNode].counter += 1
      if (local == ROOT) {
        aflag = false
      }
      if (local.asInstanceOf[InnerNode].counter > (local.asInstanceOf[InnerNode].nodesbelow / 4)) {
        rebuildhere = local.asInstanceOf[InnerNode]
      }
    }
    rebuildhere
  }


   /*
   * After rebuilding it increases the nodesbelow value of the nodes 
   * in the part of the tree that was rebuilt
   * It also fixes the rep arrays in the nodes mentioned above
   */
  def Increasenodesbelow(rebuildhere: InnerNode, leaf: LeafNode , inners: scala.collection.mutable.ListBuffer[InnerNode]) = {

    var high  :InnerNode = null
    var local :Node = null
    var aflag: Boolean = true
    var bflag: Boolean = true

    local = leaf
    high = leaf.parent

    while (aflag) {
      if (bflag) {
        if (local == high.children(0)) {
          high.asInstanceOf[Node].storedvalue = leaf.asInstanceOf[Node].storedvalue
          inners.append(high)
        } else {
          bflag = false
        }
      }
      if (high == rebuildhere) {
        bflag = false
      }

      local = local.parent
      high = high.parent
      local.asInstanceOf[InnerNode].nodesbelow += 1
      local.asInstanceOf[InnerNode].counter = 0
      if (local.parent == local) {
        aflag = false
      }
      if (local == rebuildhere) {
        aflag = false
      }
    }
  }



  /*
   * Rebuilds the subtree with the given node as root. All nodes below the said node will 
   * have their counter set to zero. It also fixes the rep/children/nodesbelow counters 
   * and arrays
   */
  def TreeRebuild(rebuildhere: InnerNode, leaves: scala.collection.mutable.ArrayBuffer[LeafNode], inners: scala.collection.mutable.ListBuffer[InnerNode], y: Double, blah: Boolean ) = {

    var frontend, rearend, anchor, degree, remain, upcounter, k, j: Int = 0
    var total: Int = 1
    var aflag = true
    var up, down = scala.collection.mutable.ArrayBuffer.empty[Node]
    var random: InnerNode = null

    frontend = leaves.indexOf(GetHead(rebuildhere, leaves))
    rearend = leaves.indexOf(GetTail(rebuildhere, leaves))
    //println("frontend rearend : " + frontend, rearend)
    remain = rearend - frontend + 1
    anchor = frontend

    if (blah) {
      inners.foreach { i =>
        i.parent = null
        i.children.clear()
        //i.REP.clear()
      }

      while (inners.nonEmpty) {
        inners.clear()
      }
    }

    if (leaves.nonEmpty && (remain != 0)) {
      rearend=rearend - k
      //println("frontend rearend : " + frontend, rearend)            

      remain = rearend - frontend + 1
      //println("remain "+remain)      

      if (remain < 4 || y == 1 ) {
        rebuildhere.nodesbelow = 0
        //rebuildhere.REP.clear()
        frontend.to(rearend).foreach { a =>
          //leaves(a).marked = false
          Increasenodesbelow(rebuildhere, leaves(a), inners)
        }
      } else {

        up.append(rebuildhere)
        anchor = rebuildhere.nodesbelow

        degree = mysqrt(rebuildhere.nodesbelow, y)
        //println("degree" + degree)
        total *= degree
        //println("Total: " + total)

        up.foreach { i =>
          i.asInstanceOf[InnerNode].children.clear()
          i.asInstanceOf[InnerNode].nodesbelow = 0
          //i.asInstanceOf[InnerNode].REP.clear()
        }
        //println("entering while")
        while (total < remain) {
          upcounter = 0
          k = 0
          0.until(total).foreach { a =>
            random = new InnerNode
            down.append(random)
            //println("new inner")
            if (k < degree) {
              CreateLink(up(upcounter).asInstanceOf[InnerNode], random, k)
              //println(upcounter , k)
              k += 1
            } else {
              k = 0
              upcounter += 1
              CreateLink(up(upcounter).asInstanceOf[InnerNode], random, k)
              //println(upcounter , k)
              k += 1
            }
          }
          while (up.nonEmpty) {
            up.clear()
          }
          down.foreach { i =>
            up.append(i)
          }
          while (down.nonEmpty) {
            down.clear()
          }
          degree = mysqrt(degree, y)
          total *= degree
        }
        //println("done while")
        upcounter = 0
        j = 0
        //println("up"+up.size)
        frontend.to(rearend).foreach { a =>
          CreateLink(up(upcounter).asInstanceOf[InnerNode], leaves(a), j)
          //println(upcounter, j)
          j += 1
          if (j == degree) {
            j = 0
            if(upcounter < (up.size -1 )){
              upcounter += 1
            }
            //println("upc"+upcounter)
          }
          //leaves(a).marked = false
          Increasenodesbelow(rebuildhere, leaves(a), inners)
        }
        if(!up(upcounter).asInstanceOf[InnerNode].children.isEmpty){
          upcounter+=1
        }
        
        upcounter.to(up.size-1).foreach{i=>
          DeleteLink(up(i).parent.asInstanceOf[InnerNode] , up(i) )
        }

      }
    } else {
      rebuildhere.counter = 0
      //rebuildhere.REP.clear()
      rebuildhere.nodesbelow = 0
    }
  }



  /*
   * Returns the leaf that we get if we always follow the leftest child
   */
  def GetHead(rebuildhere: InnerNode, leaves: scala.collection.mutable.ArrayBuffer[LeafNode]): LeafNode = {

    var local: Node = rebuildhere.children.head
    var aflag: Boolean = true

    while (aflag) {
      if (local.isInstanceOf[LeafNode]) {
        aflag = false
      } else {
        local = local.asInstanceOf[InnerNode].children.head
      }
    }
    local.asInstanceOf[LeafNode]
  }


   /*
   * Returns the leaf that we get if we always follow the rightest child
   */
  def GetTail(rebuildhere: InnerNode, leaves: scala.collection.mutable.ArrayBuffer[LeafNode]): LeafNode = {

    var local: Node = rebuildhere.children.last
    var aflag: Boolean = true

    while (aflag) {
      if (local.isInstanceOf[LeafNode]) {
        aflag = false
      } else {
        local = local.asInstanceOf[InnerNode].children.last
      }
    }
    local.asInstanceOf[LeafNode]
  }


  /*
   * Returns the number of children that the parent should have in each level 
   */
  @inline def mysqrt(u: Int, y: Double): Int = {
    if (u == 0 || u == 1 ) {
      //println(u , y)
      return 1
    } else {
      //println(u , y)
      //println("asd"+math.pow(u.toFloat, y).ceil.toInt)
      return (math.pow(u.toFloat, y).ceil.toInt)
    }
  }


  def CreateIST(y:Double , leaves: scala.collection.mutable.ArrayBuffer[LeafNode], inners: scala.collection.mutable.ListBuffer[InnerNode], iterator:Iterator[Double]):InnerNode ={
    val ROOT = (new RootNode).asInstanceOf[InnerNode]
    //ROOT.isRoot = true
    ROOT.parent = ROOT
    //println(mylist.size)
    var i:Int =0
    while(iterator.hasNext){
      var leaf = new LeafNode
      leaf.asInstanceOf[Node].storedvalue = iterator.next()
      leaf.position = i
      i+=1
      leaves.append(leaf)
      ROOT.nodesbelow += 1
      ROOT.children.append(leaf)
    }

    TreeRebuild(ROOT, leaves, inners, y , false )
    ROOT.asInstanceOf[RootNode].low = leaves.head.asInstanceOf[Node].storedvalue
    ROOT.asInstanceOf[RootNode].high = leaves(leaves.size-1).asInstanceOf[Node].storedvalue
    ROOT
  }

  def InsertAndRebuild (ROOT:InnerNode , leaves: scala.collection.mutable.ArrayBuffer[LeafNode], inners: scala.collection.mutable.ListBuffer[InnerNode], ABB:Array[Int], y:Double)={
    

    var k , l = 0

    while (( k <= (leaves.size -1)) && ( l <= (ABB.size - 1))){
      if(leaves(k).asInstanceOf[Node].storedvalue < ABB(l)){
        k+=1
      }else{
        var leaf = new LeafNode
        leaf.asInstanceOf[Node].storedvalue = ABB(l)
        //leaf.isLeaf = true
        //leaf.marked = true
        ROOT.nodesbelow += 1
        leaves(k).parent.children.insert(leaves(k).parent.children.indexOf(leaves(k)), leaf)
        leaves.insert(k , leaf)
        l+=1
        k+=1
      }
    }
    TreeRebuild(ROOT, leaves , inners , y , true)
  }
}
