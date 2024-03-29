case class EMP (Emp_id: Int, Emp_Name: String) {
if (Emp_id == 5) {
val sleepConfig=0
println(s"Will wait for ${sleepConfig} second(s)")
Thread.sleep(sleepConfig * 1000)
}
Thread.sleep(2)
}

val df = sc.makeRDD (1 to 1000).map(x => EMP (x, s"Employee Name ${x}")).toDF
df.count()
