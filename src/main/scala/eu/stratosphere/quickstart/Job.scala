package eu.stratosphere.quickstart;

import eu.stratosphere.pact.client.LocalExecutor
import eu.stratosphere.pact.client.RemoteExecutor
import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription
import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._

// You can run this locally using:
// mvn exec:exec -Dexec.executable="java" -Dexec.args="-cp %classpath eu.stratosphere.quickstart.RunJobLocal 2 file:///some/path file:///some/other/path"
object RunJobLocal {
  def main(args: Array[String]) {
    val job = new Job
    if (args.size < 3) {
      println(job.getDescription)
      return
    }
    val plan = job.getScalaPlan(args(0).toInt, args(1), args(2), args(3),  args(4))
    LocalExecutor.execute(plan)
    System.exit(0)
  }
}

class Job extends PlanAssembler with PlanAssemblerDescription with Serializable {
  override def getDescription() = {
    "Parameters: [numSubStasks] [inputCSV] [inputTSV] [output]"
  }
  override def getPlan(args: String*) = {
    getScalaPlan(args(0).toInt, args(1), args(2), args(3),  args(4))
  }

  case class Stra ( id:String, c: Int) // Stratosphere, c=classification
  case class Cas ( id:String, c: Int) // Cascading
  case class Cmp (id: String, s:Int, c:Int) // s = stra, c = cascading

  def getScalaPlan(numSubTasks: Int, input1: String, input2: String, input3:String, outPath: String) = {
    val inputOne = DataSource(input1, CsvInputFormat[Stra]() ) // was  "\n",","
    val inputTwo = DataSource(input2, CsvInputFormat[Cas]() ) // was "\n","\t"
    // val inputThree = DataSource(input2, RecordInputFormat[(String, Int)]("\n","\t") )
    //map { x => (x._1, x._2.toInt)}
   // val iTC = inputTwo 

    val err = inputOne 
    	.join(inputTwo)
    	.where { x => x.id } 
    	.isEqualTo { x => x.id } 
    	//.map { (l, r) => if(l._2 != r._2) (l._1, l._2, r._2) }
    	// .flatMap { (l, r) => if(l._2 != r._2) Some((l._1, l._2, r._2)) else None }
    	.filter { (s:Stra, c:Cas) => s.c != c.c } .map { case (s, c) => Cmp(s.id, s.c, c.c) }
//    val joined = inputOne 
//    	.join(inputTwo)
//    	.where { x => x._1 } 
//    	.isEqualTo { x => x._1 }
//    	.map { case (l, r) => (l._1, l._2, r._2) }
//    val xx = joined.join(inputThree)
//    	.where { l => l._1}
//    	.isEqualTo { r => r._1 }
//    	.map { case (l, r) => (l._1, l._2,l._3, r._2) }
//    val err = xx.filter { x => (x._2 != x._3) || (x._2 != x._4) || (x._3 != x._4) } 
   
    //val output = err.write(outPath, DelimitedOutputFormat({ (cust: String, c1: Int, c2: Int) => "%s %d %d".format(cust, c1, c2)}))
  //  val output = err.write(outPath, DelimitedOutputFormat({ x => "%s %d %d %d".format(x._1, x._2, x._3, x._4)}))
    	 val output = err.write(outPath, DelimitedOutputFormat({ x => "%s s:%d c:%d".format(x.id, x.s, x.c)}))
    	
    val plan = new ScalaPlan(Seq(output), "Wrong C Numbers")
    plan.setDefaultParallelism(numSubTasks)
    plan
  }
}
