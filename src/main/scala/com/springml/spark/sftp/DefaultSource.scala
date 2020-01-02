/*
 * Copyright 2015 springml
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.springml.spark.sftp

import java.io.File
import java.util.UUID

import com.springml.spark.sftp.util.MyFTPClient
import com.springml.spark.sftp.util.Utils.ImplicitDataFrameWriter
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
 * Datasource to construct dataframe from a sftp url
 */
class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider  {
  @transient val logger = Logger.getLogger(classOf[DefaultSource])

  /**
   * Copy the file from SFTP to local location and then create dataframe using local file
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]):BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
   * Copy the file from SFTP to local location and then create dataframe using local file
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType) = {
    val username = parameters.get("username")
    val password = parameters.get("password")
    val pemFileLocation = parameters.get("pem")
    val pemPassphrase = parameters.get("pemPassphrase")
    val host = parameters.getOrElse("host", sys.error("SFTP Host has to be provided using 'host' option"))
    val port = parameters.get("port")
    val path = parameters.getOrElse("path", sys.error("'path' must be specified"))
    val fileType = parameters.getOrElse("fileType", sys.error("File type has to be provided using 'fileType' option"))
    val inferSchema = parameters.get("inferSchema")
    val header = parameters.getOrElse("header", "true")
    val delimiter = parameters.getOrElse("delimiter", ",")
    val quote = parameters.getOrElse("quote", "\"")
    val escape = parameters.getOrElse("escape", "\\")
    val multiLine = parameters.getOrElse("multiLine", "false")
    val createDF = parameters.getOrElse("createDF", "true")
    val copyLatest = parameters.getOrElse("copyLatest", "false")
    val tempFolder = parameters.getOrElse("tempLocation", System.getProperty("java.io.tmpdir"))
    val hdfsTemp = parameters.getOrElse("hdfsTempLocation", tempFolder)
    val cryptoKey = parameters.getOrElse("cryptoKey", null)
    val cryptoAlgorithm = parameters.getOrElse("cryptoAlgorithm", "AES")
    val rowTag = parameters.getOrElse(constants.xmlRowTag, null)

    val supportedFileTypes = List("csv", "json", "avro", "parquet", "txt", "xml","orc")
    if (!supportedFileTypes.contains(fileType)) {
      sys.error("fileType " + fileType + " not supported. Supported file types are " + supportedFileTypes)
    }

    val inferSchemaFlag = if (inferSchema != null && inferSchema.isDefined) {
      inferSchema.get
    } else {
      "false"
    }

    val ftpClient = getFTPClient(username, password, host, port)
    val copiedFileLocation = copy(ftpClient, path, tempFolder, copyLatest.toBoolean)
    val fileLocation = copyToHdfs(sqlContext, copiedFileLocation, hdfsTemp)

    if (!createDF.toBoolean) {
      logger.info("Returning an empty dataframe after copying files...")
      createReturnRelation(sqlContext, schema)
    } else {
      DatasetRelation(fileLocation, fileType, inferSchemaFlag, header, delimiter, quote, escape, multiLine, rowTag, schema,
        sqlContext)
    }
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {

    val username = parameters.get("username")
    val password = parameters.get("password")
    val pemFileLocation = parameters.get("pem")
    val pemPassphrase = parameters.get("pemPassphrase")
    val host = parameters.getOrElse("host", sys.error("SFTP Host has to be provided using 'host' option"))
    val port = parameters.get("port")
    val path = parameters.getOrElse("path", sys.error("'path' must be specified"))
    val fileType = parameters.getOrElse("fileType", sys.error("File type has to be provided using 'fileType' option"))
    val header = parameters.getOrElse("header", "true")
    val copyLatest = parameters.getOrElse("copyLatest", "false")
    val tmpFolder = parameters.getOrElse("tempLocation", System.getProperty("java.io.tmpdir"))
    val hdfsTemp = parameters.getOrElse("hdfsTempLocation", tmpFolder)
    val cryptoKey = parameters.getOrElse("cryptoKey", null)
    val cryptoAlgorithm = parameters.getOrElse("cryptoAlgorithm", "AES")
    val delimiter = parameters.getOrElse("delimiter", ",")
    val quote = parameters.getOrElse("quote", "\"")
    val escape = parameters.getOrElse("escape", "\\")
    val multiLine = parameters.getOrElse("multiLine", "false")
    val codec = parameters.getOrElse("codec", null)
    val rowTag = parameters.getOrElse(constants.xmlRowTag, null)
    val rootTag = parameters.getOrElse(constants.xmlRootTag, null)

    val supportedFileTypes = List("csv", "json", "avro", "parquet", "txt", "xml","orc")
    if (!supportedFileTypes.contains(fileType)) {
      sys.error("fileType " + fileType + " not supported. Supported file types are " + supportedFileTypes)
    }

    val ftpClient = getFTPClient(username, password,  host, port)
    val tempFile = writeToTemp(sqlContext, data, hdfsTemp, tmpFolder, fileType, header, delimiter, quote, escape, multiLine, codec, rowTag, rootTag)

    upload(tempFile, path, ftpClient)
    return createReturnRelation(data)
  }
  private def copyToHdfs(sqlContext: SQLContext, fileLocation : String,
                         hdfsTemp : String): String  = {
    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val hdfsPath = new Path(hdfsTemp)
    val fs = hdfsPath.getFileSystem(hadoopConf)
    if ("hdfs".equalsIgnoreCase(fs.getScheme)) {
      var file = new File(fileLocation);
      if(file.isDirectory){
        var files = file.listFiles();
        files.foreach(entry => {
          fs.copyFromLocalFile(new Path(entry.getPath), new Path(hdfsTemp))
          val filePath = hdfsTemp + "/" + entry.getName
          fs.deleteOnExit(new Path(filePath))
        })
        return hdfsTemp;
      }else{
        fs.copyFromLocalFile(new Path(fileLocation), new Path(hdfsTemp))
        val filePath = hdfsTemp + "/" + new File(fileLocation).getName
        fs.deleteOnExit(new Path(filePath))
        return filePath
      }

    } else {
      return fileLocation
    }
  }

  private def copyFromHdfs(sqlContext: SQLContext, hdfsTemp : String,
                           fileLocation : String): String  = {
    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val hdfsPath = new Path(hdfsTemp)
    val fs = hdfsPath.getFileSystem(hadoopConf)
    if ("hdfs".equalsIgnoreCase(fs.getScheme)) {
      fs.copyToLocalFile(new Path(hdfsTemp), new Path(fileLocation))
      fs.deleteOnExit(new Path(hdfsTemp))
      return fileLocation
    } else {
      return hdfsTemp
    }
  }

  private def upload(source: String, target: String, ftpClient: MyFTPClient) {
    logger.info("Copying " + source + " to " + target)
    ftpClient.copyToFTP(source, target)
  }

  private def getFTPClient(
      username: Option[String],
      password: Option[String],
      host: String,
      port: Option[String] ) : MyFTPClient = {

    val ftpPort = if (port != null && port.isDefined) {
      port.get.toInt
    } else {
      21
    }


      new MyFTPClient(getValue(username),getValue(password),host, ftpPort)

  }

  private def createReturnRelation(data: DataFrame): BaseRelation = {
    createReturnRelation(data.sqlContext, data.schema)
  }

  private def createReturnRelation(sqlContextVar: SQLContext, schemaVar: StructType): BaseRelation = {
    new BaseRelation {
      override def sqlContext: SQLContext = sqlContextVar
      override def schema: StructType = schemaVar
    }
  }

  private def copy(ftpClient: MyFTPClient, source: String,
      tempFolder: String, latest: Boolean): String = {
    var copiedFilePath: String = null
    try {
      val target = tempFolder + File.separator + FilenameUtils.getName(source)
      copiedFilePath = target
      if (latest) {
        copiedFilePath = ftpClient.copyLatest(source, tempFolder)
      } else {
        logger.info("Copying " + source + " to " + target)
        copiedFilePath = ftpClient.copy(source, target)
      }

      copiedFilePath
    } finally {
      addShutdownHook(copiedFilePath)
    }
  }

  private def getValue(param: Option[String]): String = {
    if (param != null && param.isDefined) {
      param.get
    } else {
      null
    }
  }

  private def writeToTemp(sqlContext: SQLContext, df: DataFrame,
                          hdfsTemp: String, tempFolder: String, fileType: String, header: String,
                          delimiter: String, quote: String, escape: String, multiLine: String, codec: String, rowTag: String, rootTag: String) : String = {
    val randomSuffix = "spark_sftp_connection_temp_" + UUID.randomUUID
    val hdfsTempLocation = hdfsTemp + File.separator + randomSuffix
    val localTempLocation = tempFolder + File.separator + randomSuffix

    addShutdownHook(localTempLocation)

    fileType match {

      case "xml" =>  df.coalesce(1).write.format(constants.xmlClass)
                    .option(constants.xmlRowTag, rowTag)
                    .option(constants.xmlRootTag, rootTag).save(hdfsTempLocation)
      case "csv" => df.coalesce(1).
                    write.
                    option("header", header).
                    option("delimiter", delimiter).
                    option("quote", quote).
                    option("escape", escape).
                    option("multiLine", multiLine).
                    optionNoNull("codec", Option(codec)).
                    csv(hdfsTempLocation)
      case "txt" => df.coalesce(1).write.text(hdfsTempLocation)
      case "avro" => df.coalesce(1).write.format("com.databricks.spark.avro").save(hdfsTempLocation)
      case _ => df.coalesce(1).write.format(fileType).save(hdfsTempLocation)
    }

    copyFromHdfs(sqlContext, hdfsTempLocation, localTempLocation)
    copiedFile(localTempLocation)
  }

  private def addShutdownHook(tempLocation: String) {
    logger.debug("Adding hook for file " + tempLocation)
    val hook = new DeleteTempFileShutdownHook(tempLocation)
    Runtime.getRuntime.addShutdownHook(hook)
  }

  private def copiedFile(tempFileLocation: String) : String = {
    val baseTemp = new File(tempFileLocation)
    val files = baseTemp.listFiles().filter { x =>
      (!x.isDirectory()
        && !x.getName.contains("SUCCESS")
        && !x.isHidden()
        && !x.getName.contains(".crc")
        && !x.getName.contains("_committed_")
        && !x.getName.contains("_started_")
        )
    }
    files(0).getAbsolutePath
  }

}
