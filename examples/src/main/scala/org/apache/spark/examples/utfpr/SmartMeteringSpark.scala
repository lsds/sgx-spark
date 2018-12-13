package org.apache.spark.examples.utfpr;

//http
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.logging._

import org.apache.http.HttpHeaders
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost}
import org.apache.http.impl.client._
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.util.parsing.json.{JSONArray, JSONObject}

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

object SmartMeteringSpark {

  object Rest{

    //val DATASTORES_URL = "https://127.0.0.1:12345/"
    val DATASTORES_URL = "http://192.168.10.135:5000/datastores/"
    val API_KEY = "ApiKey 0131Byd7N220T32qp088kIT53ryT113i"
    val CODE_OK_POST = 201
    val CODE_OK_GET = 200
    val CODE_CONFLICT_POST = 409

    var DATASTORE : String = ""

    //a definir
    val MAX_ATTEMPTS = 3
    val MAX_DELAY_MS = 1000
    val TIMEOUT_MS = 10000
    //

    val LOGGER = Logger.getLogger("Log Rest")
    val currentDate = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime())
    val fh = new FileHandler("logs/" + currentDate + "_worker.log")
    fh.setFormatter(new SimpleFormatter())
    LOGGER.addHandler(fh)

    val httpClient = HttpClientBuilder.create.setDefaultRequestConfig(RequestConfig.custom.setConnectTimeout(TIMEOUT_MS).build).build
    //var httpClient = HttpClientBuilder.create().build()

    @throws(classOf[Exception])
    def post(uri : String, data : String, content_type : String, max_attempts : Int, delay_ms : Int) : Int = {

      LOGGER.info("POST " + uri + " delayed for " + delay_ms + " ms");
      Thread.sleep(delay_ms)

      //TESTE DE ERRO
      if (new Random().nextInt(5) == 1) {
        LOGGER.info("POST " + uri + " => 400")
        return 400
      }

      val t0 = System.nanoTime()

      var resp : Int = -1
      var i = 0
      while (i < max_attempts && (resp != CODE_OK_POST && resp != CODE_CONFLICT_POST)) {
        var httpPost = new HttpPost(uri)
        httpPost.addHeader(HttpHeaders.AUTHORIZATION, API_KEY)
        var strEntity = new StringEntity(data)
        strEntity.setContentType(content_type)
        httpPost.setEntity(strEntity)

        try {
          val response = httpClient.execute(httpPost)
          LOGGER.info("POST " + uri + " => attempt " + i + " : " + response.getStatusLine().getStatusCode() + " " + response.getStatusLine().getReasonPhrase)
          resp = response.getStatusLine().getStatusCode()
        }catch{
          case e: Exception => {
            LOGGER.info("POST " + uri + " => attempt " + i + " : Exception: " + e.getMessage())
            resp = -1
          }
        }

        httpPost.releaseConnection()
        i=i+1
      }

      LOGGER.info("POST " + uri + " => time: " + (System.nanoTime() - t0)/1000000 + "ms")

      return resp
    }

    @throws(classOf[Exception])
    def get(uri : String, max_attempts : Int, delay_ms : Int) : (Int, String) = {

      LOGGER.info("GET " + uri + " delayed for " + delay_ms + " ms");
      Thread.sleep(delay_ms)

      //TESTE DE ERRO
      if (new Random().nextInt(5) == 1) {
        LOGGER.info("GET " + uri + " => 400")
        return (400, "")
      }

      var resp : Int = -1
      var resp_body = ""
      var i = 0
      while (i < max_attempts && resp != CODE_OK_GET) {
        var httpGet = new HttpGet(uri)
        httpGet.addHeader(HttpHeaders.AUTHORIZATION, API_KEY)

        try {
          val response = httpClient.execute(httpGet)
          LOGGER.info("GET " + uri + " => attempt " + i + " : " + response.getStatusLine().getStatusCode() + " " + response.getStatusLine().getReasonPhrase)
          resp = response.getStatusLine().getStatusCode()
          resp_body = EntityUtils.toString(response.getEntity)

        }catch{
          case e: Exception => {
            LOGGER.info("GET " + uri + " => attempt " + i + " : Exception: " + e.getMessage())
            resp = -1
            resp_body = ""
          }
        }

        httpGet.releaseConnection()
        i=i+1
      }

      return (resp,resp_body)
    }



    def createDatastore(storage_policy : String) : Int = {
      return post(DATASTORES_URL, "name=" + DATASTORE.substring(0,DATASTORE.length-1) + "&storage_policy_name=" + storage_policy, "application/x-www-form-urlencoded", MAX_ATTEMPTS,0)
    }

    def sendCompanyData(v : (String, scala.collection.mutable.ListBuffer[String])) : Int = {
      //envia sequencia, hash e totalizadores
      val resp = post(DATASTORES_URL + DATASTORE + v._1 + ";0", v._2.mkString(", "), "application/json", MAX_ATTEMPTS, new Random().nextInt(MAX_DELAY_MS))
      return resp
    }

    def sendCompanyTot(v : (String, ((String, String), (Double, Double, Double)))) : Int = {
      //envia sequencia, hash e totalizadores
      //    seq                    hash                   tot1                    tot2                    tot3
      val str = v._2._1._1 + ";" + v._2._1._2 + ";" + v._2._2._1 + ";" + v._2._2._2 + ";" + v._2._2._3
      val resp = post(DATASTORES_URL + DATASTORE + v._1 + ";1", str, "application/json", MAX_ATTEMPTS, new Random().nextInt(MAX_DELAY_MS))
      return resp
    }

    def getCompanies() : (Int, List[String]) = {
      //retorna as empresas que existem no datastore
      val response = get(DATASTORES_URL + DATASTORE ,MAX_ATTEMPTS,new Random().nextInt(MAX_DELAY_MS))
      var companies : List[String] = null
      if (response._1 == CODE_OK_GET)
        companies = scala.util.parsing.json.JSON.parseFull(response._2).get.asInstanceOf[Map[String,List[Map[String,String]]]].get("objects").get.map(x=>x.get("key").get)
      //companies = scala.util.parsing.json.JSON.parseFull(response._2).get.asInstanceOf[Map[String,List[Map[String,String]]]].get("objects").get.map(x=>x.get("key").get.split(";")(0))

      return (response._1, companies)
    }

    def getCompanyData(company : String) : (Int, scala.collection.mutable.ListBuffer[String]) = {
      val response = get(DATASTORES_URL + DATASTORE + company + ";0" ,MAX_ATTEMPTS,new Random().nextInt(MAX_DELAY_MS))
      var lb = new scala.collection.mutable.ListBuffer[String]
      if (response._1 == CODE_OK_GET)
        response._2.split(", ").foreach(lb.append(_))

      return (response._1, lb)
    }

    def httpClientClose() : Unit = {
      httpClient.close()
    }
  }


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

    val LOGGER = Logger.getLogger("Log")
    val currentDate = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime())
    val fh = new FileHandler("logs/" + currentDate + "_master.log")
    fh.setFormatter(new SimpleFormatter())
    LOGGER.addHandler(fh)

    val conf = new SparkConf().setAppName("AppTeste").setMaster("local[3]")
    val sc = new SparkContext(conf)


    Rest.DATASTORE = "2018-11/" //sempre com barra no final

    //cria datastore se não existe
    val r = Rest.createDatastore("storage_policy_1")
    if (r == Rest.CODE_CONFLICT_POST){
      LOGGER.info("Datastore " + Rest.DATASTORE + " already exists")
    }
    else if (r != Rest.CODE_OK_POST){
      LOGGER.info("Could not create datastore " + Rest.DATASTORE + " ERROR CODE = " + r)
      System.exit(-1)
    }
    else LOGGER.info("Datastore " + Rest.DATASTORE + " created")


    //manda dados e totalizadores que estao no arquivo
    LOGGER.info("send_all_data() returned " + send_all_data("phasor/*.txt", "output/not_sent_data"))
    LOGGER.info("send_remaining_totalizers() returned " + send_remaining_totalizers())

    //manda dados que falharam anteriormente
    //LOGGER.info("send_remaining_data() returned " + send_remaining_data("output/not_sent_data", "output/not_sent_data2"))
    //LOGGER.info("send_remaining_totalizers() returned " + send_remaining_totalizers())


    Rest.httpClientClose()

    /////////

    //envia os dados que estao no rdd e salva as falhas em remaining_file
    def send_rdd_data(sm_final :  RDD[(String, ListBuffer[String])], remaining_file : String):Int = {

      //send data
      val http_data_responses = sm_final.map(x => (x._1, Rest.sendCompanyData((x._1,x._2))))
      http_data_responses.persist()

      LOGGER.info("http_data_responses: " + http_data_responses.collect().mkString("\n"))

      //salva os que deram erro ao enviar
      val not_sent_data = http_data_responses.filter(x => x._2 != Rest.CODE_OK_POST && x._2 != Rest.CODE_CONFLICT_POST).join(sm_final).map(x=>(x._1,x._2._2))
      not_sent_data.saveAsObjectFile(remaining_file)

      if (not_sent_data.collect().length > 0) //se alguma empresa não conseguiu enviar dados retorna -1
        return -1
      else
        return 0
    }

    //envia dados de todas as empresas a partir do arquivo
    def send_all_data(path : String, outputRemaining : String): Int = {
      // Solution 2
      // Getting all files in a directory:
      // It creates a map: full filename -> content of a file
      val sm_rdd = sc.textFile(path)

      // map the name of companies (column 0)
      val sm_company_contents = sm_rdd.map(a_line => (a_line.split(",")(0), a_line))

      // Agregate all contents by company
      val initial_set        = scala.collection.mutable.ListBuffer.empty[String]
      val inner_aggregation  = (s: scala.collection.mutable.ListBuffer[String], v: String) => s += v
      val outer_aggregation  = (s: scala.collection.mutable.ListBuffer[String], v: scala.collection.mutable.ListBuffer[String]) => s ++= v

      val sm_final           = sm_company_contents.aggregateByKey(initial_set)(inner_aggregation, outer_aggregation)

      LOGGER.info("sm_company_contents: " + sm_company_contents.collect.mkString("\n"))
      LOGGER.info("sm_final: " + sm_final.collect.mkString("\n"))

      return send_rdd_data(sm_final, outputRemaining)

    }

    //envia totalizadores de todas empresas que já enviaram os dados mas não mandaram os totalizadores
    def send_remaining_totalizers(): Int = {

      //get companies
      val get_result = Rest.getCompanies()
      if (get_result._1 != Rest.CODE_OK_GET) {
        LOGGER.info("get companies failed (" + get_result._1 + ") - ABORTING")
        return get_result._1
      }

      val companies = sc.parallelize(get_result._2).map(x=>(x.split(";")(0),x.split(";")(1).toInt)).aggregateByKey(0)((s:Int, v:Int) => s+v, (s:Int, v:Int) => s+v).filter(x=>x._2 == 0).map(x=>x._1)

      //get companies data (sm_final_get deve ser igual sm_final); filtrando os que deram erro no get
      val sm_final_get = companies.map(x=>(x,Rest.getCompanyData(x))).filter(x=>x._2._1 == Rest.CODE_OK_GET).map(x=>(x._1,x._2._2))
      sm_final_get.persist()

      LOGGER.info("companies: " + companies.collect.mkString("\n"))
      LOGGER.info("sm_final_get: " + sm_final_get.collect.mkString("\n"))

      // getting totalizers for specific columns
      // val totalizer_1 = sm_final.mapValues(x => x.map(y => y.split(";")(18))).mapValues(y => y.flatMap(s => scala.util.Try(s.toDouble).toOption).reduce(_+_))
      // val totalizer_2 = sm_final.mapValues(x => x.map(y => y.split(";")(20))).mapValues(y => y.flatMap(s => scala.util.Try(s.toDouble).toOption).reduce(_+_))
      // val totalizer_3 = sm_final.mapValues(x => x.map(y => y.split(";")(21))).mapValues(y => y.flatMap(s => scala.util.Try(s.toDouble).toOption).reduce(_+_))

      // getting totalizers for specific columns by company => clean version
      def get_values_from_columns(content: String) : (Double, Double, Double) = {
        val splitted = content.split(",")
        return (splitted(21).toDouble, splitted(22).toDouble, splitted(23).toDouble)
      }

      val totalizer = sm_final_get.mapValues(x => x.map(y => get_values_from_columns(y))).mapValues(x => x.reduce((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3)))

      // signing all content by company
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
      //val sm_company_verify = sm_company_contents_get.mapValues(a_line => (a_line, calculateLineHash(a_line) == get_order_and_hash(a_line)._2))
      val sm_company_verify = sm_company_contents_get.mapValues(a_line => (a_line, Inmetro_cipher.verify(get_hash_content(a_line), get_order_and_hash(a_line)._2)))
      val sm_final_verify = sm_company_verify.mapValues(x=>x._2).aggregateByKey(true)((s: Boolean, v: Boolean) =>  s && v, (s: Boolean, v: Boolean) =>  s && v)
      val failed_hashes = sm_company_verify.filter(x=>x._2._2==false)

      // merging totalizer, hash_and_sign, and sm_final and filter failed hashes in order to send it to chocolatecloud
      //val merged = orders_and_hashes.join(totalizer).join(sm_final).join(sm_final_verify)
      val merged = orders_and_hashes.join(totalizer).join(sm_final_get).join(sm_final_verify).filter(x=>x._2._2 == true).mapValues(x=>x._1)


      LOGGER.info("totalizer: " + totalizer.collect.mkString("\n"))
      LOGGER.info("orders_and_hashes: " + orders_and_hashes.collect.mkString("\n"))
      LOGGER.info("failed_hashes: " + failed_hashes.collect.mkString("\n"))
      LOGGER.info("sm_final_verify: " + sm_final_verify.collect.mkString("\n"))


      val http_tot_responses = merged.map(x => (x._1, Rest.sendCompanyTot((x._1,x._2._1))))
      http_tot_responses.persist()

      LOGGER.info("http_tot_responses: " + http_tot_responses.collect().mkString("\n"))

      if (http_tot_responses.filter(x=>(x._2!=Rest.CODE_OK_POST)).collect().length > 0) //se alguma empresa não conseguiu enviar totalizadores retorna -1
        return -1
      else
        return 0

    }

    //envia dados das empresas que falharam anteriormente
    def send_remaining_data(inputRemaining : String, outputRemaining : String) : Int = {

      val sm_final = sc.objectFile[(String, scala.collection.mutable.ListBuffer[String])](inputRemaining)
      LOGGER.info("sm_final: " + sm_final.collect.mkString("\n"))
      return send_rdd_data(sm_final, outputRemaining)
    }

  }
}
