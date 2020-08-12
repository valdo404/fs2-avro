package com.adikteev.fs2

import cats.effect.IO
import com.aktv.test.TestMessage
import fs2.Stream
import org.apache.avro.file.CodecFactory
import org.specs2.matcher.{IOMatchers, ResultMatchers}
import org.specs2.mutable.Specification

import scala.concurrent.ExecutionContext

class PipedStreamsSpec extends Specification with IOMatchers with ResultMatchers {
  "stream record data using pipedinputstream" >> {
    "should be piped accordingly" >> {
      val scoreFixtures: Seq[TestMessage] =
        Seq(
          new TestMessage("aa1", 2.0d),
          new TestMessage("aa2", 2.0d),
          new TestMessage("aa3", 2.0d))

      val avroPipes = SpecificPipes[IO, TestMessage]

      val ec = ExecutionContext.global

      val readFileOp: IO[List[TestMessage]] = Stream
        .emits(scoreFixtures)
        .through(avroPipes.toAvroFile(ec, CodecFactory.bzip2Codec()))
        .through(avroPipes.fromAvroFile)
        .compile
        .toList

      readFileOp.unsafeRunSync().size should beEqualTo(3)
    }
  }
}
