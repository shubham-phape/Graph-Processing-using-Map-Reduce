import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
case class Vertex( Vid: Long, group:Long, adjacent:List[Long])
object Graph {
  def main(args: Array[ String ]): Unit = {
    val config = new SparkConf().setAppName("Graph")
    config.setMaster("local[2]")
    val sc = new SparkContext( config )

    var node = sc.textFile(args(0)).map( node => {

      //separating each column form line by comma and converting it to list
      val k = node.split(  "," ).toList

      //Entering tuples values Node ID, Group Id, adjacents nodes in the Vertex
      Vertex ( k.head.toLong, k.head.toLong ,  k.tail.map(_.toLong) )
    })

    for (i <- 1 to 5){
      node = node.flatMap( v => v.adjacent.flatMap( a =>
        /*Mapping each adjacent node with group number and actual node with its group number*/
        Seq(( a, v.group ))) ++ Seq(( v.Vid, v.group )))

        // reducing according to the minimum group
        .reduceByKey( ( p,q ) => p  min q )

        //clubbing the previous nodes and the group mapped nodes
        .join(  node.map( v => ( v.Vid, v )))

        // matching the type eith the needed tuples
        .map{ case( vertexId , ( min, vertex )) => Vertex( vertexId, min, vertex.adjacent ) }
    }

    // Printing the output here
    val result = node.map( v => ( v.group, 1 ))
      .reduceByKey(( x, y ) => ( x + y ))
    //for all tuples
    result.map(x => {

      x._1 + "\t" + x._2

    }).collect().foreach(println)

    sc.stop()


  }
}
