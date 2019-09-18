package comp9313.proj2
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object Problem1 {
  def main(args: Array[String]) {
  		val inputFile = args(0)
  		val outputFolder = args(1)
  		val conf = new
  		SparkConf().setAppName("Problem1").setMaster("local")
  		val sc = new SparkContext(conf)
  		val input = sc.textFile(inputFile)
  		
  		//get the lines in the file
  		val lines = input.filter(!_.isEmpty()).map(_.toLowerCase().split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+"))
  		
  		//get pairs of (wi,wj) in each line
  	  val pairs = lines.map(line=>
  	    for(i <-0 until line.length ;j<-i+1 until line.length) yield (line(i),line(j))
  	  )
  	  // get all the pairs in the file
  		val all_pairs = pairs.flatMap(x=>x)
  		
  		//remove items starting with non-alphabetical characters
  		val allpairs =all_pairs.filter(x=>x._1.split("")(0).matches("[a-z]")).filter(x=>x._2.split("")(0).matches("[a-z]"))
  		
  		//calculate relative frequency 
  		val res = allpairs.groupBy(_._1).mapValues(w =>    
  		  w.groupBy(identity).mapValues(_.size.toDouble/w.size)  
  		  )
  		  .flatMap(_._2)    
  		  
  		// ((wi,wj),tf) => (wi,wj,tf)
  		val resnew =res.map(x=> (x._1._1,x._1._2,x._2))
  		//sort the results
  		val res_sorted = resnew.sortBy(x => (x._1,-x._3,x._2))
  		
  		val final_res = res_sorted.map(x=>x._1+" "+x._2+" "+x._3)
  		final_res.saveAsTextFile(outputFolder)
  		
  }//end main
}