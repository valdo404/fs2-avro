package com.levaldo.fs2

import java.io.File

import cats.effect.{Blocker, IO}
import com.levaldo.test.TestMessage
import fs2.{Pure, Sink, Stream}
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

      val avroPipes: AvroPipes[IO, TestMessage] = SpecificPipes.apply[IO, TestMessage]

      val blocker = Blocker.apply[IO]

      blocker.use(blocker => {
        val ioToMessage: Sink[IO, TestMessage] = avroPipes.fileThroughSink(
          fs2.io.file.writeAll(temp.toPath, blocker), ec)

        ioToMessage(Stream
          .emits(scoreFixtures))
          .compile
          .drain
        }
      ).unsafeRunSync()

      val reader: DataFileReader[Nothing] = new DataFileReader[Nothing](temp, new GenericDatumReader[Nothing])

      val metadatas = reader.getMetaKeys.asScala
        .map(key => (key, reader.getMetaString(key)))

      metadatas must havePair(
        "avro.schema" -> """{"type":"record","name":"TestMessage","namespace":"com.levaldo.test","fields":[{"name":"test","type":"string"},{"name":"value","type":"double"}]}"""
      )
      metadatas must havePair("avro.codec" -> "snappy")

      temp.exists() must beTrue

    }
  }
}
