package org.spark.learn

object listReplace {
	def main(args:Array[String]):Unit={
			val listint = List(1,2,3,4)
					listint.take(2).foreach(println) 

					val proclist = listint.filter(x => x >2)
					val multiply = listint.map(x => x * 2)

					val liststr = List ("King","Walhuke","Kingston","Tyler")
					//val filterlist = liststr.filter(x => x.contains("King"))
					val filterlist = liststr.map( x => x +",sai")
					val replacelist = filterlist.map( x => x.replace("sai","DP") )
					replacelist.foreach(println)

					val listtilde = List("Durga~P","Prasad~D","D~Prasad")
					val flatlist = listtilde.flatMap(x => x.split("~"))
					val flatfilter = flatlist.filter(x => x.contains("Prasad"))
					val replflat = flatfilter.map(x => x.replace("Prasad","CH"))
					replflat.foreach(println)
	}
}