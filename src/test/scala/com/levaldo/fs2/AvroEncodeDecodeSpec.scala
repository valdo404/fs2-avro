package com.adikteev.fs2

import java.io.File

import cats.effect.IO
import com.aktv.test.TestMessage
import fs2.Stream
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.GenericDatumReader
import org.specs2.matcher.{IOMatchers, ResultMatchers}
import org.specs2.mutable.Specification

import scala.concurrent.ExecutionContext

class AvroEncodeDecodeSpec extends Specification with IOMatchers with ResultMatchers {
  "should be able to store avro records to a (closed) file" >> {
    "test file was stored" >> {
      import scala.collection.JavaConverters._

      val scoreFixtures: Seq[TestMessage] =
        Seq(new TestMessage("aa", 2.0d), new TestMessage("bb", 2.0d), new TestMessage("cc", 2.0d))

      val temp = File.createTempFile("tempfile", ".avro")

      val ec = ExecutionContext.global

      val avroPipes = SpecificPipes.apply[IO, TestMessage]

      Stream
        .emits(scoreFixtures)
        .to(avroPipes.fileThroughSink(fs2.io.file.writeAll(temp.toPath, ec), ec))
        .compile
        .drain
        .unsafeRunSync()

      val reader: DataFileReader[Nothing] = new DataFileReader[Nothing](temp, new GenericDatumReader[Nothing])

      val metadatas = reader.getMetaKeys.asScala
        .map(key => (key, reader.getMetaString(key)))

      metadatas must havePair(
        "avro.schema" -> """{"type":"record","name":"TestMessage","namespace":"com.aktv.test","fields":[{"name":"test","type":"string"},{"name":"value","type":"double"}]}"""
      )
      metadatas must havePair("avro.codec" -> "snappy")

      temp.exists() must beTrue

    }
  }
}
