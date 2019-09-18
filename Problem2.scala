package comp9313.proj2

import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Problem2 {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("kcycle").setMaster("local")
    val sc = new SparkContext(conf)
    
    val fileName = args(0)
    val k = args(1).toInt
    val edges = sc.textFile(fileName)   
    
    //create graph
    val edgelist = edges.map(x => x.split(" ")).map(x=> Edge(x(1).toLong, x(2).toLong,true))
    val graph = Graph.fromEdges(edgelist,true)
    
	  //initialGraph
    val initialGraph = graph.mapVertices( (id,attr) => List[List[VertexId]]()) 
  
    //send message function
    def sendMsg(triplet: EdgeTriplet[List[List[VertexId]],Boolean]):Iterator[(VertexId,List[List[VertexId]])]={
       if(triplet.srcAttr.length==0){
         Iterator((triplet.dstId, triplet.srcAttr ++ List(List(triplet.srcId))))
       }
       else{
         Iterator((triplet.dstId, triplet.srcAttr.map(e => e++List(triplet.srcId))))  
       }
    }
    
    //pregel
    val res = initialGraph.pregel(List[List[VertexId]](),k)(
      (id, ns, newns) => ns++newns, // Vertex Program
      sendMsg,//Send Message
      (a, b) => a++b // Merge Message
    )
    
    //paths of length k
    val res1 = res.vertices.map{case(id,attr) =>(id,attr.filter(a=>a.length==k))}
    //paths start from the verticeId
    val res2 = res1.map{case(id,attr) =>(id,attr.filter(a=>a(0)==id))}
    //list to set,avoid cycles containing smaller cycles
    val res3 = res2.map{case(id,attr)=>(id,attr.map(_.toSet))}
    //paths of length k
    val res4 = res3.map{case(id,attr)=>(id,attr.filter(a=>a.size==k))}
    //remove the duplicate paths
    val res5 = res4.flatMap{case(id,attr)=> attr}.distinct()
    //the number of cycles
    println(res5.count)
  }
}