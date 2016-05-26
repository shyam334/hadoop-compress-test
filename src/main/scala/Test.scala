
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{Writable, SequenceFile}
import org.apache.hadoop.io.SequenceFile.{CompressionType, Reader, Writer}
import org.apache.hadoop.io.compress.{SnappyCodec, CompressionCodec}

import scopt.{OptionParser, Read}
import scopt.Read.reads

import scala.collection.JavaConverters._
import scala.util.{Failure, Try}

import Test._

object Test {

  implicit val pathRead: Read[Path] = reads(new Path(_))

  def createReader(path: Path, conf: Configuration = new Configuration()): Reader =
    if(FileSystem.get(conf).isFile(path)) {
      println(FileSystem.get(conf).getContentSummary(path))

      new Reader(conf, Reader.file(path))
    }
    else
      sys.error(s"ERROR: The $path is not a file.")

  def createWriter(path:              Path,
                   keyClass:          Class[_],
                   valueClass:        Class[_],
                   compressionType:   CompressionType   = CompressionType.BLOCK,
                   compressionCodec:  CompressionCodec  = new SnappyCodec(),
                   conf:              Configuration     = new Configuration()): Writer =
    SequenceFile.createWriter(conf,
      Writer.file(path),
      Writer.keyClass(keyClass),
      Writer.valueClass(valueClass),
      Writer.compression(compressionType, compressionCodec))

  def write(r: Reader, w: Writer) = {
    //Dragons and more.
    val key   = instanceOfClassAsWritable(r.getKeyClass)
    val value = instanceOfClassAsWritable(r.getValueClass)

    Stream
      .continually(r.next(key, value))
      .takeWhile(_ != false)
      .foreach(_ => w.append(key, value))
  }

  def printer(file: String, x: Reader) =
    println(s"""|Info of file $file
                |Key class          : ${x.getKeyClassName}
                |Value class        : ${x.getValueClassName}
                |Compressed         : ${x.isCompressed}
                |Block compressed   : ${x.isBlockCompressed}
                |Compression Type   : ${x.getCompressionType}
                |Compression Codec  : ${x.getCompressionCodec.getCompressorType}
                | ${x.getMetadata.getMetadata.asScala.mkString("\n")}
             """.stripMargin)

  def instanceOfClassAsWritable(x: Class[_]): Writable =
    x.newInstance().asInstanceOf[Writable]

  implicit class SafeOps[T](x: T) {
    def safe = Try(x) match {
      case Failure(y) => sys.error(s"ERROR: $y")
      case x          => x.toOption
    }
    def safeI = Try(x).toOption
  }
}

object TestRunner {

  case class Config(inFile: Option[Path] = None, outFile: Option[Path] = None)

  def main(args: Array[String]) {
    val parser = new OptionParser[Config]("hadoop-test") {
      head("hadoop-compress-test", "0.1.1")
      arg[Path]("<input file>")   action ((x, c) => c.copy(inFile  = Some(x)))  text "input file"
      arg[Path]("<output file>")  action ((x, c) => c.copy(outFile = Some(x)))  text "output file"
    }

    parser.parse(args, Config()) match {
      case Some(config) => run(config)
      case None         => sys.exit(1)
    }
  }

  def run(c: Config) = {
    runSequence(c)
  }

  def runSequence(c: Config) = {
    val r   = for {
                    file              <-  c.inFile
                    reader            <-  createReader(file).safe
                    _                 =   printer(file.toString, reader)
                  } yield reader

    val w   = for {
                    reader            <-  r
                    oFile             <-  c.outFile
                    keyType           <-  reader.getKeyClass.safe
                    valType           <-  reader.getValueClass.safe
                    writer            <-  createWriter(oFile, keyType, valType).safe
                  } yield writer

    val len = for {
                    reader            <-  r
                    writer            <-  w
                    l                 <-  write(reader, writer).safe
                    _                 <-  writer.hsync().safe
                    _                 <-  writer.hflush().safe
                  } yield l
  }
}
