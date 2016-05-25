
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{DataOutputBuffer, Writable, SequenceFile}
import org.apache.hadoop.io.SequenceFile.{CompressionType, Reader, Writer}
import org.apache.hadoop.io.compress.{SnappyCodec, CompressionCodec, BZip2Codec}
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat.WritableValueBytes
import scopt.Read
import scopt.Read.reads

import scala.reflect.ClassTag
import scala.util.{Failure, Try}
import scala.collection.JavaConverters._

import Test._

object Test {

  def createReader(path: Path, conf: Configuration = new Configuration()): Reader =
    if(FileSystem.get(conf).isFile(path)) {
      println(FileSystem.get(conf).getContentSummary(path))
      //println(FileSystem.get(conf).listCorruptFileBlocks())
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
/*    val keyBytes  = new DataOutputBuffer()
    val valBytes  = new WritableValueBytes()

    Stream
      .continually(r.nextRaw(keyBytes, valBytes))
      .takeWhile(_ != -1)
      .map{x => w.appendRaw(keyBytes.getData, 0, keyBytes.getLength, valBytes); x}
      .sum*/

    //WTFHadoop
    val key   = instanceOfClassAsWritable(r.getKeyClass)
    val value = instanceOfClassAsWritable(r.getValueClass)
    Stream
      .continually(r.next(key, value))
      .takeWhile(_ != false)
      .foreach(_ => w.append(key, value))
  }

  def printer(file: String, x: Reader) =
    println(s"""|Info of file ${file}
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
    def safe: Option[T] = Try(x) match {
      case Failure(y) => sys.error(s"ERROR: $y")
      case x          => x.toOption
    }
    def safeI: Option[T] = Try(x).toOption
  }
}

object TestRunner {

  case class Config(inFile: Option[Path] = None, outFile: Option[Path] = None)
  implicit val pathRead: Read[Path] = reads(new Path(_))

  def main(args: Array[String]) {
    val parser = new scopt.OptionParser[Config]("hadoop-test") {
      head("hadoop-test", "0.1")
      arg[Path]("<input file>") action{(x, c) => c.copy(inFile = Some(x))} text("input file")
      arg[Path]("<output file>") action((x, c) => c.copy(outFile = Some(x))) text("output file")
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
      // do stuff

      case None =>
      // arguments are bad, error message will have been displayed
    }

    val arg = for {
                    fromFile          <-  Try(new Path(args(0))).toOption
                    toFile            <-  Try(new Path(args(1))).toOption
                  } yield (fromFile, toFile)

    val r   = for {
                    file              <-  arg.map(_._1)
                    reader            <-  createReader(file).safe
                    _                 =   printer(file.toString, reader)
                  } yield reader

    val w   = for {
                    reader            <-  r
                    keyType           <-  reader.getKeyClass.safe
                    valType           <-  reader.getValueClass.safe
                    key               <-  instanceOfClassAsWritable(keyType).safe
                    value             <-  instanceOfClassAsWritable(valType).safe
                    oFile             <-  arg.map(_._2)
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
