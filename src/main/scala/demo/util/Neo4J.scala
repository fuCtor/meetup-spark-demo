package demo.util

import demo.Context
import org.apache.spark.sql.DataFrame
import org.neo4j.spark.{Neo4jConfig, Neo4jDataFrame}

import scala.collection.JavaConverters._

object Neo4J {
  // https://github.com/neo4j-contrib/neo4j-spark-connector/blob/master/src/main/scala/org/neo4j/spark/Neo4jDataFrame.scala
  def saveNodes(dataFrame: DataFrame, name: String, id: String)(implicit ctx: Context): Unit = {
    val mergeStatement =
      s"""
      UNWIND {rows} as row
      MERGE (n:`$name` {`$id` : row.`$id`}) SET n += row
      """

    val indexStatements = Seq(
      s"CREATE INDEX ON :`$name`(`$id`)"
    )

    val partitions = Math.max(1, (dataFrame.count() / 10000).asInstanceOf[Int])
    val config = neo4jConfig()

    indexStatements.foreach(
      Neo4jDataFrame.execute(config, _, Map.empty[String, AnyRef].asJava)
    )
    dataFrame.repartition(partitions).foreachPartition(rows => {
      val params: AnyRef = rows.map(r =>
        r.schema.map(c => (c.name, r.getAs[AnyRef](c.name))).toMap.asJava
      ).asJava
      Neo4jDataFrame.execute(config, mergeStatement, Map("rows" -> params).asJava)
    })
  }

  // https://github.com/neo4j-contrib/neo4j-spark-connector/blob/master/src/main/scala/org/neo4j/spark/Neo4jDataFrame.scala
  def saveEdges(dataFrame: DataFrame,
                source: (String, String),
                relationship: (String, Seq[String]),
                target: (String, String))(implicit ctx: Context): Unit = {
    val mergeStatement =
      s"""
      UNWIND {rows} as row
      MATCH (source:`${source._1}` {`${source._2}` : row.source.`${source._2}`})
      MATCH (target:`${target._1}` {`${target._2}` : row.target.`${target._2}`})
      MERGE (source)-[rel:`${relationship._1}`]->(target) SET rel += row.relationship
      """
    val partitions = Math.max(1, (dataFrame.count() / 10000 + 1).toInt)
    val config = neo4jConfig()

    val rdd = dataFrame.repartition(partitions).rdd

    for (part <- rdd.partitions) {
      val idx = part.index

      val data = rdd.mapPartitionsWithIndex((index, it) => {
        if (index == idx) it
        else Iterator.empty
      }, preservesPartitioning = true).collect()
      val params: AnyRef = data.toIterator.map(r =>
        Map(
          "source" -> Map(source._2 -> r.getAs[AnyRef](source._2)).asJava,
          "target" -> Map(target._2 -> r.getAs[AnyRef](target._2)).asJava,
          "relationship" -> relationship._2.map(c => (c, r.getAs[AnyRef](c))).toMap.asJava)
          .asJava).asJava
      println(s"Save to ${relationship._1} $idx part")
      Neo4jDataFrame.execute(config, mergeStatement, Map("rows" -> params).asJava)
    }
  }

  def neo4jConfig()(implicit ctx: Context) = Neo4jConfig(ctx.sparkConfig.get("spark.neo4j.bolt.url", "bolt://localhost"))
}
