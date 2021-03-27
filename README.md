# Graph-Processing-using-Map-Reduce
Detecting Graph Connectedness using Map Reduce in Java and on Scala aswell 

• Developed a Map-Reduce algorithm to find the graph connectedness of any undirected graphs to be used with SDSC Comet
supercomputer cluster.

• Implemented this algorithm using Hadoop Map- Reduce in Java.

• Later implemented the algorithm in Spark and Scala to achieve 50% - 60% improvement in speed.



<h2>The Pseudo code for the implementation is using multiple Mappers is given below:</h2>
<p  style="color:red">
class Vertex extends Writable {

  short tag;                 // 0 for a graph vertex, 1 for a group number
  
  long group;                // the group where this vertex belongs to
  
  long VID;                  // the vertex ID
  
  Vector adjacent;     // the vertex neighbors
  
  ...
  
}
</p>



<b>First Map-Reduce job:</b>

map ( key, line ) =

  parse the line to get the vertex VID and the adjacent vector
  
  emit( VID, new Vertex(0,VID,VID,adjacent) )
  
  
<b>Second Map-Reduce job:</b>

map ( key, vertex ) =

  emit( vertex.VID, vertex )   // pass the graph topology
  
  for n in vertex.adjacent:
  
     emit( n, new Vertex(1,vertex.group) )  // send the group # to the adjacent vertices

reduce ( vid, values ) =

  m = Long.MAX_VALUE;
  
  for v in values {
  
     if v.tag == 0
     
        then adj = v.adjacent.clone()     // found the vertex with vid
        
     m = min(m,v.group)
     
  }
  
  emit( m, new Vertex(0,m,vid,adj) )      // new group #
 
 
<b>Final Map-Reduce job:</b>

map ( group, value ) =

   emit(group,1)
   
reduce ( group, values ) =

   m = 0
   
   for v in values
   
       m = m+v
       
   emit(group,m)
