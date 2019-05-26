package org.alexeyn

import java.nio.file.Paths
import java.util.concurrent.Executors

import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.instances.int._
import cats.syntax.all._
import fs2._

import scala.concurrent.ExecutionContext
import scala.util.matching.Regex

object Main extends IOApp {
  val Separator = ";"
  val isNextId: Regex = s"^(\\d+.*)$Separator".r
  val LastRowPaddingId = s"1$Separator"
  val ColumnsInFile = 10
  val HeaderLines = 1

  override def run(args: List[String]): IO[ExitCode] =
    processFile("input.csv").compile.drain.as(ExitCode.Success)

  private val blockingExecutionContext =
    Resource.make(IO(ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))))(ec => IO(ec.shutdown()))

  private def processFile(filePath: String): Stream[IO, Unit] = {
    Stream.resource(blockingExecutionContext).flatMap { blockingEC =>
      io.file
        .readAll[IO](Paths.get(filePath), blockingEC, 4 * 4096)
        .through(text.utf8Decode)
        .append(Stream.eval(IO.pure("\n" + LastRowPaddingId)))
        .through(text.lines)
        .drop(HeaderLines)
        .scan(("", "")) {
          case ((acc, _), line) =>
            val tuple = concatBrokenLines(acc, line)
            //println(tuple)
            tuple
        }
        .filter { case (_, line) => line.trim.nonEmpty }
        .map { case (_, line) => line.split(Separator, ColumnsInFile) }
        .map(processRow)
        .foldMap(_ => 1)
        .map(n => println(s"Processed $n record(s)"))
    }
  }

  private def concatBrokenLines(acc: String, line: String) = {
    // next line detected, i.e. we flush `acc` downstream since it already contains complete line to be processed
    if (isNextId.findFirstIn(acc).isDefined && isNextId.findFirstIn(line).isDefined) (line, acc)
    // next line is not yet detected, i.e. we flush empty string and append current line to the current `acc` state
    else (acc.replace("\n", " ") + line, "")
  }

  private def processRow(columns: Array[String]): Unit = {
    println(s"processed: ${columns.mkString(" :: ")}")
  }
}
