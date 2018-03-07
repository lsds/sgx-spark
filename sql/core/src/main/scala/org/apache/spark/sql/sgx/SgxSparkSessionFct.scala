package org.apache.spark.sql.sgx

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.SparkConf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder

import org.apache.spark.sgx.SgxMessage

object SgxSparkSessionFct {

	// Builder

	def newBuilder() = NewBuilder().send()

	def builderConfig(builder: Builder, key: String, value: String) = BuilderConfigString(builder, key, value).send()

    def builderConfig(builder: Builder, key: String, value: Long) = BuilderConfigLong(builder, key, value).send()

    def builderConfig(builder: Builder, key: String, value: Double) = BuilderConfigDouble(builder, key, value).send()

    def builderConfig(builder: Builder, key: String, value: Boolean) = BuilderConfigBoolean(builder, key, value).send()

    def builderConfig(builder: Builder, conf: SparkConf) = BuilderConfigConf(builder, conf).send()

	def builderGetOrCreate(builder: Builder) = BuilderGetOrCreate(builder).send()

	// DataFrameReader

	def dataFrameReaderFormat(reader: DataFrameReader, source: String) = DataFrameReaderFormat(reader, source).send()

	def dataFrameReaderLoad(reader: DataFrameReader, path: String) = DataFrameReaderLoad(reader, path).send()

	def dataFrameReaderOption(reader: DataFrameReader, key: String, value: String) = DataFrameReaderOptionString(reader, key, value).send()

	def dataFrameReaderOption(reader: DataFrameReader, key: String, value: Boolean) = DataFrameReaderOptionBoolean(reader, key, value).send()

	def dataFrameReaderOption(reader: DataFrameReader, key: String, value: Long) = DataFrameReaderOptionLong(reader, key, value).send()

	def dataFrameReaderOption(reader: DataFrameReader, key: String, value: Double) = DataFrameReaderOptionDouble(reader, key, value).send()

	// DataSet

	def datasetRdd[T](dataset: Dataset[T]) = DatasetRdd(dataset).send()

	// SparkSession

	def sparkSessionRead(session: SparkSession) = SparkSessionRead(session).send()
}

/*
 * Builder
 */

private[sql] case class NewBuilder() extends SgxMessage[Builder] {
	def execute() = Await.result( Future { SparkSession.builder }, Duration.Inf)
}

private[sql] case class BuilderConfigString(builder: Builder, key: String, value: String) extends SgxMessage[Builder] {
	def execute() = Await.result( Future { builder.getBuilder.config(key, value) }, Duration.Inf)
}

private[sql] case class BuilderConfigLong(builder: Builder, key: String, value: Long) extends SgxMessage[Builder] {
	def execute() = Await.result( Future { builder.getBuilder.config(key, value) }, Duration.Inf)
}

private[sql] case class BuilderConfigDouble(builder: Builder, key: String, value: Double) extends SgxMessage[Builder] {
	def execute() = Await.result( Future { builder.getBuilder.config(key, value) }, Duration.Inf)
}

private[sql] case class BuilderConfigBoolean(builder: Builder, key: String, value: Boolean) extends SgxMessage[Builder] {
	def execute() = Await.result( Future { builder.getBuilder.config(key, value) }, Duration.Inf)
}

private[sql] case class BuilderConfigConf(builder: Builder, conf: SparkConf) extends SgxMessage[Builder] {
	def execute() = Await.result( Future { builder.getBuilder.config(conf) }, Duration.Inf)
}

private[sql] case class BuilderGetOrCreate(builder: Builder) extends SgxMessage[SparkSession] {
	def execute() = Await.result( Future { builder.getBuilder.getOrCreate() }, Duration.Inf)
}


/*
 * DataFrameReader
 */

private[sql] case class DataFrameReaderFormat(reader: DataFrameReader, source: String) extends SgxMessage[DataFrameReader] {
	def execute() = Await.result( Future { reader.getDataFrameReader.format(source) }, Duration.Inf)
}

private[sql] case class DataFrameReaderLoad(reader: DataFrameReader, path: String) extends SgxMessage[DataFrame] {
	def execute() = Await.result( Future { reader.getDataFrameReader.load(path) }, Duration.Inf)
}

private[sql] case class DataFrameReaderOptionString(reader: DataFrameReader, key: String, value: String) extends SgxMessage[DataFrameReader] {
	def execute() = Await.result( Future { reader.getDataFrameReader.option(key, value) }, Duration.Inf)
}

private[sql] case class DataFrameReaderOptionBoolean(reader: DataFrameReader, key: String, value: Boolean) extends SgxMessage[DataFrameReader] {
	def execute() = Await.result( Future { reader.getDataFrameReader.option(key, value) }, Duration.Inf)
}

private[sql] case class DataFrameReaderOptionLong(reader: DataFrameReader, key: String, value: Long) extends SgxMessage[DataFrameReader] {
	def execute() = Await.result( Future { reader.getDataFrameReader.option(key, value) }, Duration.Inf)
}

private[sql] case class DataFrameReaderOptionDouble(reader: DataFrameReader, key: String, value: Double) extends SgxMessage[DataFrameReader] {
	def execute() = Await.result( Future { reader.getDataFrameReader.option(key, value) }, Duration.Inf)
}


/*
 * DataSet
 */

private[sql] case class DatasetRdd[T](dataset: Dataset[T]) extends SgxMessage[RDD[T]] {
	def execute() = Await.result( Future { dataset.getDataset.rdd }, Duration.Inf)
}


/*
 * SparkSession
 */

private[sql] case class SparkSessionRead(session: SparkSession) extends SgxMessage[DataFrameReader] {
	def execute() = Await.result( Future { session.getSparkSession.read }, Duration.Inf)
}
