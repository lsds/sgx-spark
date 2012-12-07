package spark.rdd

import spark._
import scala.Some

private[spark] class ShuffledRDDSplit(val idx: Int) extends Split {
  override val index = idx
  override def hashCode(): Int = idx
}

/**
 * The resulting RDD from a shuffle (e.g. repartitioning of data).
 * @param prev the parent RDD.
 * @param part the partitioner used to partition the RDD
 * @tparam K the key class.
 * @tparam V the value class.
 */
class ShuffledRDD[K, V](
    prev: RDD[(K, V)],
    part: Partitioner)
  extends RDD[(K, V)](prev.context, List(new ShuffleDependency(prev, part))) {

  override val partitioner = Some(part)

  @transient
  var splits_ = Array.tabulate[Split](part.numPartitions)(i => new ShuffledRDDSplit(i))

  override def splits = splits_

  override def compute(split: Split): Iterator[(K, V)] = {
    val shuffledId = dependencies.head.asInstanceOf[ShuffleDependency[K, V]].shuffleId
    SparkEnv.get.shuffleFetcher.fetch[K, V](shuffledId, split.index)
  }

  override def changeDependencies(newRDD: RDD[_]) {
    dependencies_ = List(new OneToOneDependency(newRDD.asInstanceOf[RDD[Any]]))
    splits_ = newRDD.splits
  }
}
