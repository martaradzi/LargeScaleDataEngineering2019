// group08

def reorg(datadir :String) {
  //val t0 = System.nanoTime()

  val person   = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").load(datadir + "/person.*csv.*")
  val interest = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").load(datadir + "/interest.*csv.*")
  val knows    = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").load(datadir + "/knows.*csv.*")

  // mutual friendship check
  val df1_temp = knows.withColumnRenamed("personId","personId_df1")
  val df1 = df1_temp.withColumnRenamed("friendId","friendId_df1")
  val df2_temp = knows.withColumnRenamed("personId","personId_df2")
  val df2 = df2_temp.withColumnRenamed("friendId", "friendId_df2")
  val knows_filtered = df1.join(df2, df1.col("personId_df1") === df2.col("friendId_df2") ).filter($"personId_df2" === $"friendId_df1").drop("personId_df2","friendId_df2")
  val knows_new_1 = knows_filtered.withColumnRenamed("personId_df1","personId")
  val knows_new_2 = knows_new_1.withColumnRenamed("friendId_df1","friendId")

  val person_clean = person.drop("gender","locationIP","firstName","lastName","creationDate","browserUsed")

  val knows_join_person = person_clean.join(knows_new_2, "personId")
  val knows_id = knows_join_person.withColumnRenamed("personId", "tempPersonId")
  val knows_bday = knows_id.withColumnRenamed("birthday", "tempBday")
  val knows_loc = knows_bday.withColumnRenamed("locatedIn", "tempLocation")

  // mutual location check
  val knows_filtered_1 = knows_loc.join(person_clean, knows_loc.col("friendId") === person_clean.col("personId")).
    filter($"tempLocation" === $"locatedIn").drop("tempBday", "tempLocation", "friendId", "birthday", "locatedIn").distinct
  
  // filtering single people in one location 
  val knows_filtered_2 = knows_filtered_1.withColumnRenamed("personId", "friendId")
  
  val person_filtered_1 = person_clean.join(knows_filtered_2, person_clean.col("personId") === knows_filtered_2.col("tempPersonId")) 
  val person_final = person_filtered_1.select($"personId", $"birthday", $"locatedIn").distinct

  val knows_final = knows_filtered_2.withColumnRenamed("tempPersonId", "personId")

  // join on interest
  val temp = person_final.join(interest, "personId")

  // save/overwrite files, in parquet format
  temp.write.format("parquet").mode("overwrite").save(datadir + "/person.parquet")
  knows_final.write.format("parquet").mode("overwrite").save(datadir + "/knows.parquet")

  spark.catalog.clearCache()

  //val t1 = System.nanoTime()
  //println("reorg time: " + (t1 - t0)/1000000 + "ms")
}

def cruncher(datadir :String, a1 :Int, a2 :Int, a3 :Int, a4 :Int, lo :Int, hi :Int) :org.apache.spark.sql.DataFrame =
{
  val t0 = System.nanoTime() 

  // select the relevant (personId, interest) tuples, and add a boolean column "nofan" (true iff this is not a a1 tuple)
  val person = spark.read.parquet(datadir + "/person.parquet/*").cache()
  val focus = person.filter($"interest" isin (a1, a2, a3, a4)).withColumn("nofan", $"interest".notEqual(a1))
  person.unpersist()

  // compute person score (#relevant interests): join with focus, groupby (birthday first) & aggregate. Note: nofan=true iff person does not like a1
  //val scores = focus.groupBy("birthday", "personId").agg(count("personId") as "score", min("nofan") as "nofan")
  val scores = focus.groupBy("birthday", "personId").agg(count("personId") as "score", min("nofan") as "nofan")
  focus.unpersist();

  // filter (personId, score) tuples with score>1, being nofan, and having the right birthdate
  val cands = scores.filter($"score" > 0 && $"nofan").withColumn("bday", month($"birthday")*100 + dayofmonth($"birthday")).filter($"bday" >= lo && $"bday" <= hi)

  // create (personId, ploc, friendId, score) pairs by joining with knows
  val knows = spark.read.parquet(datadir + "/knows.parquet/*").cache()
  val pairs = cands.select($"personId", $"score").join(knows, "personId")
  knows.unpersist()
  cands.unpersist()

  // re-use the scores dataframe to create a (friendId, floc) dataframe of persons who are a fan (not nofan)
  val fanlocs = scores.filter(!$"nofan").select($"personId".alias("friendId"))
  scores.unpersist()

  // join the pairs to get a (personId, ploc, friendId, floc, score), and remove ploc and floc columns
  val results = pairs.join(fanlocs, "friendId").select($"personId".alias("p"), $"friendId".alias("f"), $"score")
  pairs.unpersist()
  fanlocs.unpersist()

  // keep only the (p, f, score) columns and sort the result
  val ret = results.select($"p", $"f", $"score").orderBy(desc("score"), asc("p"), asc("f"))
  spark.catalog.clearCache()

  ret.show(1000) // force execution now, and display results to stdout

  val t1 = System.nanoTime()
  println("cruncher time: " + (t1 - t0)/1000000 + "ms")

  return ret
}
