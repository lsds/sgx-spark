package org.apache.spark.examples.utfpr;

//logging
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.logging._

//http
import org.apache.http.HttpHeaders
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client._
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils

//spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

//utils
import scala.collection.mutable.ListBuffer
import scala.util.Random

// for hashing
import java.security.MessageDigest
import java.math.BigInteger

// for RSA ciphering
import java.nio.charset.StandardCharsets.UTF_8
import java.security._
import java.security.spec.X509EncodedKeySpec
import java.security.spec.RSAPrivateCrtKeySpec
import javax.crypto._
import javax.crypto.spec.SecretKeySpec
import javax.crypto.spec.IvParameterSpec
import sun.security.util.DerInputStream
import sun.security.util.DerValue
import org.apache.commons.codec.binary.Base64
import org.apache.commons.codec.binary.Hex

object SmartMeteringSparkFileMode {

  object Inmetro_cipher {
    // object to carry out all ciphering procedures used in smartmeter demo.
    // based on:
    //    example to cipher: https://gist.github.com/urcadox/6173812
    //    example to verify signature: https://stackoverflow.com/questions/11691462/verifying-a-signature-with-a-public-key
    // here, inmetro public and private keys are hard-coded
    // private key SHOULD BE PROVIDED by remote/local attestation
    val str_inmetro_private_key =
    """-----BEGIN RSA PRIVATE KEY-----
      MIICXAIBAAKBgQCtTOinaknnhjhll+PIdjGsS933KNW5wW2KtH7vTbmmWN6Z5GKB
      6NO0esLX7phALcjkXPBGNniCmHWgyQU4MD0L8zjWD3lXcmBQFPERiVXTli0kMS+C
      4Eqi6AiTVP3QUWt6GDMNN9oVyzunUfW2de8iEtIpV6nT/joJxAQGUDBKhQIDAQAB
      AoGBAJbDT6tFhmHKnImVZ+5fFLuljMaWWciuA9QlTkB1R8r1iUIsM558pKBgI92i
      jgVXT2uLhuQuQwyqAbsM5mOJY7DxPO4nLq/RjV9Mo//L48ltOmabgFBcf1n0UL/m
      Gbbin4QHYJONplvb2+A59XunOrffD20zghycEKQCcW1aptXBAkEA0Y+V1eKD1H9u
      1KYTE5qFFEyr8JA6IhyVg1ReHrxlJ2LC4delfYt537HP4fNeYKQytVeyGcH7nMG/
      MQ5G6RqLeQJBANO0Q24uxuZMftGNOKudc03IfkK6ehqLBdesx70mTYTU4PUfLfBX
      dPHe5vAtNWOnGCnWC5LKQTlwdc8hUhzYKG0CQD7kvg5sJi6fdE7j7PPEO06FWFEh
      qCDWvVSl/H1zA1TXwi9vvh44vwIQ5pgkp12Pyhw8zpoGaxJ3337EjymkqtkCQEko
      6oSRNbswhEL2grcL2mTu/HMi7j9t+77kvsHnErLsvjD3bXC5SHithzFI7kJZ5EfQ
      6H751kB7VLsX0MCXEqECQE3erdyehcs/iRTO3DyAv2wE/s05rBtLSui5oOJZQ89A
      7PkX9gNgzQ7zvw1a2Po2wPDeqkv0XKvv8X2joTIy5fU=
      -----END RSA PRIVATE KEY-----"""

    val str_inmetro_public_key =
      """-----BEGIN PUBLIC KEY-----
        MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCtTOinaknnhjhll+PIdjGsS933
        KNW5wW2KtH7vTbmmWN6Z5GKB6NO0esLX7phALcjkXPBGNniCmHWgyQU4MD0L8zjW
        D3lXcmBQFPERiVXTli0kMS+C4Eqi6AiTVP3QUWt6GDMNN9oVyzunUfW2de8iEtIp
        V6nT/joJxAQGUDBKhQIDAQAB
        -----END PUBLIC KEY-----"""

    val str_file_key = "328679D713C2E54BAB6E0DD613ABF628"
    val str_file_iv  = "7643A112F2F4090DE6A8C4516809BC89"

    def get_public_key() : PublicKey = {
      val stripped_key = str_inmetro_public_key.replace("-----BEGIN PUBLIC KEY-----\n", "").replace("\n-----END PUBLIC KEY-----", "").replaceAll("\\s", "")
      val key_spec     = new X509EncodedKeySpec(new Base64().decode(stripped_key))
      val factory      = KeyFactory.getInstance("RSA")
      return factory.generatePublic(key_spec)
    }

    def get_private_key() : PrivateKey = {
      val stripped_key = str_inmetro_private_key.replace("-----BEGIN RSA PRIVATE KEY-----\n", "").replace("\n-----END RSA PRIVATE KEY-----", "").replaceAll("\\s", "")
      val derReader    = new DerInputStream(new Base64().decode(stripped_key))
      val sequence     = derReader.getSequence(0)
      val modulus      = sequence(1).getBigInteger
      val publicExp    = sequence(2).getBigInteger
      val privateExp   = sequence(3).getBigInteger
      val prime1       = sequence(4).getBigInteger
      val prime2       = sequence(5).getBigInteger
      val exp1         = sequence(6).getBigInteger
      val exp2         = sequence(7).getBigInteger
      val crtCoef      = sequence(8).getBigInteger
      val key_spec     = new RSAPrivateCrtKeySpec(modulus, publicExp, privateExp, prime1, prime2, exp1, exp2, crtCoef)
      val factory      = KeyFactory.getInstance("RSA")
      return factory.generatePrivate(key_spec)
    }

    def get_file_key() : SecretKeySpec = {
      return new SecretKeySpec(Hex.decodeHex(str_file_key.toCharArray), "AES")
    }

    def get_file_iv() : IvParameterSpec = {
      return new IvParameterSpec(Hex.decodeHex(str_file_iv.toCharArray))
    }

    def symm_encrypt(data: String): String = {
      val key    = get_file_key
      val iv     = get_file_iv
      val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
      cipher.init(Cipher.ENCRYPT_MODE, key, iv)
      return new Base64().encodeAsString(cipher.doFinal(data.getBytes("UTF-8")))
    }

    def symm_decrypt(data: String): String = {
      val key    = get_file_key
      val iv     = get_file_iv
      val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
      cipher.init(Cipher.DECRYPT_MODE, key, iv)
      return new String(cipher.doFinal(new Base64().decode(data)))
    }

    def encrypt(data: String): String = {
      val key    = get_public_key
      val cipher = Cipher.getInstance("RSA");
      cipher.init(Cipher.ENCRYPT_MODE, key)
      return new Base64().encodeAsString(cipher.doFinal(data.getBytes("UTF-8")))
    }

    def decrypt(data: String): String = {
      val key    = get_private_key
      val cipher = Cipher.getInstance("RSA");
      cipher.init(Cipher.DECRYPT_MODE, key)
      return new String(cipher.doFinal(new Base64().decode(data)))
    }

    def sign(message: String): String = {
      val key    = get_private_key
      val signer = Signature.getInstance("SHA256withRSA")
      signer.initSign(key)
      signer.update(message.getBytes("UTF-8"))
      return new Base64().encodeAsString(signer.sign())
    }

    def verify(message: String, signature: String): Boolean = {
      val key    = get_public_key
      val signer = Signature.getInstance("SHA256withRSA")
      signer.initVerify(key)
      signer.update(message.getBytes("UTF-8"))
      return signer.verify(new Base64().decode(signature))
    }
  }


  def main(args: Array[String]) {

    //spark context
    //val conf = new SparkConf().setAppName("AppTeste").setMaster("local[3]")
    val conf = new SparkConf()
    val sc = new SparkContext(conf)


    //inicializa o logger do master

    val LOGGER = Logger.getLogger("Log")
    // no need for this logger file handler, we already log everything
    /*
    val currentDate = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime())
    val fh = new FileHandler("/home/paublin/workspace/sgx-spark/phasor/master.log")
    fh.setFormatter(new SimpleFormatter())
    LOGGER.addHandler(fh)
    */

    //leitura args

    var mode = "0"  //0 => envia dados;  1 => envia totalizadores;  2 => envia dados remanescentes
    if (args.length > 0) mode = args(0)
    LOGGER.info("Mode: " + mode)

    if (mode == "0") {
      if (args.length != 3) {
        LOGGER.info("Invalid arguments - Required: 0 <path_to_store_files> <path_to_input_file>")
        return
      }
    }
    else if (mode == "1") {
      if (args.length != 2) {
        LOGGER.info("Invalid arguments - Required: 1 <path_to_store_files>")
        return
      }
    }
    else{
      LOGGER.info("Invalid mode.")
      return
    }

    //val PATH_TO_SAVE = "hdfs://radlab-hadoop:9000/testespark/2018-11"
    val PATH_TO_SAVE = args(1)

    //faz agregacao ou totalizacao e salva em arquivos
    if (mode == "0") {
      //LOGGER.info("send_all_data() returned " + send_all_data("/home/giovanni/phasor/*.txt"))
      LOGGER.info("send_all_data() returned " + send_all_data(args(2)))
    }
    else if (mode == "1") {
      LOGGER.info("send_totalizers() returned " + send_totalizers())
    }


    /////////
    //Funções auxiliares
    /////////

    //cria o rdd com a agregacao das empresas a partir do arquivo e salva arquivo _data
    def send_all_data(path : String): Int = {

      // Getting all files in a directory:
      // It creates a map: full filename -> content of a file
      val sm_rdd = sc.textFile(path)

      println("send_all_data 1 " + path)

      // map the name of companies (column 0)
      val sm_company_contents = sm_rdd.map(a_line => (a_line.split(",")(0), a_line))

      println("send_all_data 2")

      // Agregate all contents by company
      val initial_set        = scala.collection.mutable.ListBuffer.empty[String]
      println("send_all_data 3")
      val inner_aggregation  = (s: scala.collection.mutable.ListBuffer[String], v: String) => s += v
      println("send_all_data 4")
      val outer_aggregation  = (s: scala.collection.mutable.ListBuffer[String], v: scala.collection.mutable.ListBuffer[String]) => s ++= v
      println("send_all_data 5")

      val sm_final           = sm_company_contents.aggregateByKey(initial_set)(inner_aggregation, outer_aggregation)
      println("send_all_data 6")

      LOGGER.info("sm_company_contents: " + sm_company_contents.collect.mkString("\n"))
      println("send_all_data 7")
      LOGGER.info("sm_final: " + sm_final.collect.mkString("\n"))
      println("send_all_data 8")

      try {
        println("send_all_data 9")
        sm_final.saveAsObjectFile(PATH_TO_SAVE + "data/")
        println("send_all_data 10")
      }
      catch{
        case e: Exception => {
          LOGGER.info(e.getMessage)
          return -1
        }
      }
      println("send_all_data 11")

      return 0
    }

    //salva totalizadores de todas empresas a partir do arquivo _data
    def send_totalizers(): Int = {

      var sm_final_get : RDD[(String, scala.collection.mutable.ListBuffer[String])] = null
      try {
        sm_final_get = sc.objectFile[(String, scala.collection.mutable.ListBuffer[String])](PATH_TO_SAVE + "data/")
        sm_final_get.persist()
        LOGGER.info("sm_final_get: " + sm_final_get.collect.mkString("\n"))
      }
      catch{
        case e: Exception => {
          LOGGER.info(e.getMessage)
          return -1
        }
      }

      // getting totalizers for specific columns by company => clean version
      def get_values_from_columns(content: String) : (Double, Double, Double) = {
        val splitted = content.split(",")
        return (splitted(21).toDouble, splitted(22).toDouble, splitted(23).toDouble)
      }

      val totalizer = sm_final_get.mapValues(x => x.map(y => get_values_from_columns(y))).mapValues(x => x.reduce((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3)))

      def get_order_and_hash(content: String) : (String, String) = {
        val splitted = content.split(",")
        return (splitted(28) + "-" + splitted(29) + ";", splitted(27))
      }

      def hash_and_sign(content: String) : String = {
        return String.format("%032x", new BigInteger(1, MessageDigest.getInstance("SHA-256").digest(content.getBytes("UTF-8"))))
      }

      val orders_and_hashes = sm_final_get.mapValues(x => x.map(y => get_order_and_hash(y))).mapValues(x => x.reduce((a, b) => (a._1 + b._1, a._2 + b._2))).mapValues(x => (x._1, hash_and_sign(x._2)))


      //verify hashes

      // retorna a parte da linha em que o hash foi calculado e assinado anteriormente
      def get_hash_content(line : String) : String = {
        return line.split(",").slice(1,27).mkString(",") + ",'"
        //return line.split(",").slice(1,27).mkString("")
      }

      //maps company name, hash ok
      val sm_company_contents_get = sm_final_get.flatMap(x=>x._2).map(x=>(x.split(",")(0),x))
      val sm_company_verify = sm_company_contents_get.mapValues(a_line => (a_line, Inmetro_cipher.verify(get_hash_content(a_line), get_order_and_hash(a_line)._2)))
      val sm_final_verify = sm_company_verify.mapValues(x=>x._2).aggregateByKey(true)((s: Boolean, v: Boolean) =>  s && v, (s: Boolean, v: Boolean) =>  s && v)
      val failed_hashes = sm_company_verify.filter(x=>x._2._2==false)

      // merging correct hashes totalizers, hash_and_sign, and sm_final and filter failed hashes in order to send it to chocolatecloud
      val merged = orders_and_hashes.join(totalizer).join(sm_final_get).join(sm_final_verify).filter(x=>x._2._2 == true).mapValues(x=>x._1)


      LOGGER.info("totalizer: " + totalizer.collect.mkString("\n"))
      LOGGER.info("orders_and_hashes: " + orders_and_hashes.collect.mkString("\n"))
      LOGGER.info("failed_hashes: " + failed_hashes.collect.mkString("\n"))
      LOGGER.info("sm_final_verify: " + sm_final_verify.collect.mkString("\n"))

      try {
        merged.saveAsObjectFile(PATH_TO_SAVE + "tot/")
      }
      catch{
        case e: Exception => {
          LOGGER.info(e.getMessage)
          return -1
        }
      }

      return 0
    }
  }
}
