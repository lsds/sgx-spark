/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.utfpr;

// logging
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.logging._

// http
import org.apache.http.HttpHeaders
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client._
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils

// spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

// utils
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

// scalastyle:off
object SmartMeteringSpark {

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

    //Configuration

    val LOGS_DIRECTORY = "/tmp/"
    val API_KEY = "ApiKey 0131Byd7N220T32qp088kIT53ryT113i"
    //val API_KEY = "ApiKey 0131Byd7N220T32qp088kIT53ryT113i0123456789012345"
    val STORAGE_POLICY = "sparkdemo_sp"
    val FAKE_ERROR = false //para debug - simula erro aleatoriamente no post ou get

    //Tuning Variables

    val MAX_ATTEMPTS = 3 //numero maximo de tentativas de post ou get
    val MAX_DELAY_MS = 1000 //maximo intervalo de delay para cada requisicao http
    val TIMEOUT_MS = 10000 //timeout para tentativa de conexao

    //


    //create spark context
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
	  sc.setLogLevel("WARN")

    //inicializa o logger do master

    val LOGGER = Logger.getLogger("Log")

    //leitura args
    var mode = "0"  //0 => envia dados;  1 => envia totalizadores;  2 => envia dados remanescentes
    if (args.length > 0) mode = args(0)

    if (mode == "0") {
      LOGGER.warning("Mode: 0 (Aggregation mode).")
      if (args.length != 5) {
        LOGGER.severe("Invalid arguments - Required: 0 <datastore_url> <datastore_name> <path_to_input_file> <path_to_not_sent>")
        return
      }
    }
    else if (mode == "1") {
      LOGGER.warning("Mode 1 (Billing mode).")
      if (args.length != 3) {
        LOGGER.severe("Invalid arguments - Required: 1 <datastore_url> <datastore_name>")
        return
      }
    }
    else if (mode == "2"){
      LOGGER.warning("Mode 2 (SendRemaining mode).")
      if (args.length != 5) {
        LOGGER.severe("Invalid arguments - Required: 2 <datastore_url> <datastore_name> <path_to_input_not_sent> <path_to_output_not_sent>")
        return
      }
    }
    else{
      LOGGER.severe("Invalid mode.")
      return
    }


    ////broadcast constants
    val CODE_OK_POST = sc.broadcast(201)
    val CODE_OK_GET = sc.broadcast(200)
    val CODE_CONFLICT_POST = sc.broadcast(409)
    val DATASTORES_URL = sc.broadcast(args(1))
    val DATASTORE = sc.broadcast(args(2))
    val API_KEY_B = sc.broadcast(API_KEY)
    val FAKE_ERROR_B = sc.broadcast(FAKE_ERROR)
    val MAX_ATTEMPTS_B = sc.broadcast(MAX_ATTEMPTS)
    val MAX_DELAY_MS_B = sc.broadcast(MAX_DELAY_MS)
    val TIMEOUT_MS_B = sc.broadcast(TIMEOUT_MS)

    object Rest extends java.io.Serializable{

      @throws(classOf[Exception])
      def post(uri : String, data : String, content_type : String, max_attempts : Int, delay_ms : Int) : Int = {

        val httpClient = HttpClientBuilder.create.setDefaultRequestConfig(RequestConfig.custom.setConnectTimeout(TIMEOUT_MS_B.value ).build).build

        Thread.sleep(delay_ms)

        //TESTE DE ERRO
        if (FAKE_ERROR_B.value == true){
          if (new Random().nextInt(5) == 1) return 999
        }

        var resp : Int = -1
        var i = 0
        while (i < max_attempts && (resp != CODE_OK_POST.value && resp != CODE_CONFLICT_POST.value)) {
          var httpPost = new HttpPost(uri)
          httpPost.addHeader(HttpHeaders.AUTHORIZATION, API_KEY_B.value)
          var strEntity = new StringEntity(data)
          strEntity.setContentType(content_type)
          httpPost.setEntity(strEntity)

          try {
            val response = httpClient.execute(httpPost)
            resp = response.getStatusLine().getStatusCode()
          }catch{
            case e: Exception => {
              resp = -1
            }
          }

          httpPost.releaseConnection()
          i=i+1
        }

        httpClient.close()

        return resp
      }

      @throws(classOf[Exception])
      def get(uri : String, max_attempts : Int, delay_ms : Int) : (Int, String) = {

        val httpClient = HttpClientBuilder.create.setDefaultRequestConfig(RequestConfig.custom.setConnectTimeout(TIMEOUT_MS_B.value ).build).build

        Thread.sleep(delay_ms)

        //TESTE DE ERRO
        if (FAKE_ERROR_B.value == true){
          if (new Random().nextInt(5) == 1) return (999, "")
        }

        var resp : Int = -1
        var resp_body = ""
        var i = 0
        while (i < max_attempts && resp != CODE_OK_GET.value) {
          var httpGet = new HttpGet(uri)
          httpGet.addHeader(HttpHeaders.AUTHORIZATION, API_KEY_B.value)

          try {
            val response = httpClient.execute(httpGet)
            resp = response.getStatusLine().getStatusCode()
            resp_body = EntityUtils.toString(response.getEntity)

          }catch{
            case e: Exception => {
              resp = -1
              resp_body = ""
            }
          }

          httpGet.releaseConnection()
          i=i+1
        }

        httpClient.close()

        return (resp,resp_body)
      }



      def createDatastore(storage_policy : String) : Int = {
        System.err.println("createDatastore with " + DATASTORES_URL.value + "name=" + DATASTORE.value.substring(0,DATASTORE.value.length-1) + "&storage_policy_name=" + storage_policy)
        return post(DATASTORES_URL.value, "name=" + DATASTORE.value.substring(0,DATASTORE.value.length-1) + "&storage_policy_name=" + storage_policy, "application/x-www-form-urlencoded", MAX_ATTEMPTS_B.value ,0)
      }

      def sendCompanyData(v : (String, scala.collection.mutable.ListBuffer[String])) : Int = {
        //envia sequencia, hash e totalizadores
        System.err.println("sendCompanyData on " + DATASTORES_URL.value + DATASTORE.value + v._1 + ";0")
        val resp = post(DATASTORES_URL.value + DATASTORE.value + v._1 + ";0", v._2.mkString(", "), "application/json", MAX_ATTEMPTS_B.value , new Random().nextInt(MAX_DELAY_MS_B.value ))
        return resp
      }

      def sendCompanyTot(v : (String, ((String, String), (Double, Double, Double)))) : Int = {
        //envia sequencia, hash e totalizadores
        //    seq                    hash                   tot1                    tot2                    tot3
        val str = v._2._1._1 + ";" + v._2._1._2 + ";" + v._2._2._1 + ";" + v._2._2._2 + ";" + v._2._2._3
        val resp = post(DATASTORES_URL.value + DATASTORE.value + v._1 + ";1", str, "application/json", MAX_ATTEMPTS_B.value , new Random().nextInt(MAX_DELAY_MS_B.value ))
        return resp
      }

      def getCompanies() : (Int, List[String]) = {
        //retorna as empresas que existem no datastore
        val response = get(DATASTORES_URL.value + DATASTORE.value + "?cursor=O2tleTtUcnVlOzA7MA%3D%3D",MAX_ATTEMPTS_B.value ,new Random().nextInt(MAX_DELAY_MS_B.value ))
		    //?cursor=O2tleTtUcnVlOzA7MA%3D%3D makes the kvs return the whole data instead of chunks
        var companies : List[String] = null
        if (response._1 == CODE_OK_GET.value)
          companies = scala.util.parsing.json.JSON.parseFull(response._2).get.asInstanceOf[Map[String,List[Map[String,String]]]].get("objects").get.map(x=>x.get("key").get)

        return (response._1, companies)
      }

      def getCompanyData(company : String) : (Int, scala.collection.mutable.ListBuffer[String]) = {
        val response = get(DATASTORES_URL.value + DATASTORE.value + company + ";0" ,MAX_ATTEMPTS_B.value ,new Random().nextInt(MAX_DELAY_MS_B.value ))
        var lb = new scala.collection.mutable.ListBuffer[String]
        if (response._1 == CODE_OK_GET.value)
          response._2.split(", ").foreach(lb.append(_))

        return (response._1, lb)
      }
    }


    val s_time = System.nanoTime()

    //cria datastore se ainda nao existe

// the datastore has already been created
/*
    LOGGER.warning("Creating datastore " +  DATASTORE.value)
    val r = Rest.createDatastore(STORAGE_POLICY)
    if (r == CODE_CONFLICT_POST.value){
      LOGGER.info("Datastore " + DATASTORE.value + " already exists.")
    }
    else if (r != CODE_OK_POST.value){
      LOGGER.severe("Could not create datastore " + DATASTORE.value + " ERROR CODE = " + r)
      return
    }
    else LOGGER.warning("Datastore " + DATASTORE.value + " created.")
*/

    //faz agregacao ou totalizacao e envio para o banco

    if (mode == "0") {
      val ret = send_all_data(args(3), args(4))
      LOGGER.warning("send_all_data() returned " + ret)

      if (ret != 0) LOGGER.warning("!!! Some data were not saved at KVS. Please retry in mode 2.")
      else LOGGER.warning("!!! Execution OK.")
    }

    else if (mode == "1") {
      val ret = send_remaining_totalizers()
      LOGGER.warning("send_remaining_totalizers() returned " + ret)

      if (ret != 0) LOGGER.warning("!!! Some companies could not send billing data to KVS. Please retry.")
      else LOGGER.warning("!!! Execution OK.")
    }

    else if (mode == "2"){
      val ret = send_remaining_data(args(3), args(4))
      LOGGER.warning("send_remaining_data() returned " + ret)

      if (ret != 0) LOGGER.warning("!!! Some data were not saved at KVS. Please retry.")
      else LOGGER.warning("!!! Execution OK.")
    }

    LOGGER.warning("------> Total execution time: " + ((System.nanoTime() - s_time)/1000000) + "ms")

    /////////
    //Funcões auxiliares
    /////////

    //envia os dados que estao no rdd e salva as falhas em remaining_file
    def send_rdd_data(sm_final :  RDD[(String, ListBuffer[String])], remaining_file : String):Int = {

      //send data

      val s_time = System.nanoTime()

      val http_data_responses = sm_final.map(x => (x._1, Rest.sendCompanyData((x._1,x._2))))
      http_data_responses.persist()

      LOGGER.warning("http_data_responses: " + http_data_responses.collect().mkString("\n"))

      LOGGER.warning("------> KVS storing time: " + ((System.nanoTime() - s_time)/1000000) + "ms")

      //salva os que deram erro ao enviar
      val not_sent_data = http_data_responses.filter(x => x._2 != CODE_OK_POST.value && x._2 != CODE_CONFLICT_POST.value).join(sm_final).map(x=>(x._1,x._2._2))

      if (!not_sent_data.isEmpty()) {
        if (remaining_file != "null") { //if "null", failures are not saved
          not_sent_data.saveAsTextFile(remaining_file) //use saveAsTextFile() instead of saveAsObjectFile() for running on sgx-spark
          LOGGER.warning("Not sent data were saved at " + remaining_file)
        } else {
          LOGGER.warning("Not sent data were discarded.")
        }
        return -1 //se alguma empresa nao conseguiu enviar dados retorna -1
      }
      else{
        LOGGER.warning("All data has been succesfully sent.")
        return 0
      }
    }

    //cria o rdd com a agregacao das empresas a partir do arquivo e envia os dados
    def send_all_data(path : String, outputRemaining : String): Int = {

      LOGGER.warning("Aggregating data ...")

      // Getting all files in a directory:
      // It creates a map: full filename -> content of a file
      val sm_rdd = sc.textFile(path)

	  
	    val s_time = System.nanoTime()

      // map the name of companies (column 0)
      //val sm_company_contents = sm_rdd.map(a_line => (a_line.split(",")(0), a_line))
      val sm_company_contents = sm_rdd.map(a_line => new String(new Base64().decode(a_line))).map(a_line => (a_line.split(",")(0), a_line))

      // Agregate all contents by company
      val initial_set        = scala.collection.mutable.ListBuffer.empty[String]
      val inner_aggregation  = (s: scala.collection.mutable.ListBuffer[String], v: String) => s += v
      val outer_aggregation  = (s: scala.collection.mutable.ListBuffer[String], v: scala.collection.mutable.ListBuffer[String]) => s ++= v

      val sm_final           = sm_company_contents.aggregateByKey(initial_set)(inner_aggregation, outer_aggregation)

      LOGGER.warning("------> Data processing time: " + ((System.nanoTime() - s_time)/1000000) + "ms")

      LOGGER.warning("Sending aggregated data to KVS ...")

      val result = send_rdd_data(sm_final, outputRemaining)

      //these lines are disabled because collect() function uses lot of memory and the process breaks for huge amount of data
      //LOGGER.info("sm_company_contents: " + sm_company_contents.collect().mkString("\n"))
      //LOGGER.info("sm_final: " + sm_final.collect().mkString("\n"))
	
	    return result;

    }

    //envia totalizadores de todas empresas que já enviaram os dados mas nao mandaram os totalizadores
    def send_remaining_totalizers(): Int = {

	    var s_time = System.nanoTime()

      LOGGER.warning("Getting companies from KVS ...")

      //pega todas as empresas do datastore
      val get_result = Rest.getCompanies()
      if (get_result._1 != CODE_OK_GET.value) {
        LOGGER.severe("get companies failed (" + get_result._1 + ") - ABORTING")
        return get_result._1
      }

      //empresas que enviaram dados mas nao enviaram totalizadores
      val companies = sc.parallelize(get_result._2).map(x=>(x.split(";")(0),x.split(";")(1).toInt)).aggregateByKey(0)((s:Int, v:Int) => s+v, (s:Int, v:Int) => s+v).filter(x=>x._2 == 0).map(x=>x._1)

      LOGGER.warning("Getting companies data from KVS ...")

      //pega dados das empresas; filtrando os que deram erro no get
      val sm_final_get = companies.map(x=>(x,Rest.getCompanyData(x))).filter(x=>x._2._1 == CODE_OK_GET.value).map(x=>(x._1,x._2._2))
      sm_final_get.persist()

      LOGGER.warning("------> KVS retrieving time: " + ((System.nanoTime() - s_time)/1000000) + "ms")

      s_time = System.nanoTime()

      LOGGER.warning("Calculating billing data and hashes...")

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

      val orders_and_hashes = sm_final_get.mapValues(x => x.map(y => get_order_and_hash(y))).mapValues(x => x.reduce((a, b) => (a._1 + b._1, a._2 + b._2))).mapValues(x => (x._1, Inmetro_cipher.sign(x._2)))


      //verify hashes

      // retorna a parte da linha em que o hash foi calculado e assinado anteriormente
      def get_hash_content(line : String) : String = {
        return line.split(",").slice(1,27).mkString("")
      }

      //maps company name, hash ok
      val sm_company_contents_get = sm_final_get.flatMap(x=>x._2).map(x=>(x.split(",")(0),x))
      val sm_company_verify = sm_company_contents_get.mapValues(a_line => (a_line, Inmetro_cipher.verify(get_hash_content(a_line), get_order_and_hash(a_line)._2)))
      val sm_final_verify = sm_company_verify.mapValues(x=>x._2).aggregateByKey(true)((s: Boolean, v: Boolean) =>  s && v, (s: Boolean, v: Boolean) =>  s && v)
      val failed_hashes = sm_company_verify.filter(x=>x._2._2==false)

      // merging correct hashes totalizers, hash_and_sign, and sm_final and filter failed hashes in order to send it to chocolatecloud
      val merged = orders_and_hashes.join(totalizer).join(sm_final_get).join(sm_final_verify).filter(x=>x._2._2 == true).mapValues(x=>x._1)

      LOGGER.warning("Sending billing data to KVS ...")

      val http_tot_responses = merged.map(x => (x._1, Rest.sendCompanyTot((x._1,x._2._1))))
      http_tot_responses.persist()

      http_tot_responses.collect()
      LOGGER.warning("------> Processing execution time: " + ((System.nanoTime() - s_time)/1000000) + "ms")


      //these lines are disabled because collect() function uses lot of memory and the process breaks for huge amount of data
      //LOGGER.info("companies: " + companies.collect().mkString("\n"))
      //LOGGER.info("sm_final_get: " + sm_final_get.collect().mkString("\n"))
      //LOGGER.info("totalizer: " + totalizer.collect().mkString("\n"))
      //LOGGER.info("orders_and_hashes: " + orders_and_hashes.collect().mkString("\n"))
      //LOGGER.info("failed_hashes: " + failed_hashes.collect().mkString("\n"))
      //LOGGER.info("sm_final_verify: " + sm_final_verify.collect().mkString("\n"))
      //LOGGER.info("http_tot_responses: " + http_tot_responses.collect().mkString("\n"))

      if (http_tot_responses.filter(x=>(x._2!=CODE_OK_POST.value)).collect().length > 0) //se alguma empresa nao conseguiu enviar totalizadores retorna -1
        return -1
      else
        return 0

    }

    //envia dados das empresas que falharam anteriormente a partir do arquivo
    def send_remaining_data(inputRemaining : String, outputRemaining : String) : Int = {

      //function to convert string to desired object value
      def textToObj(str : String) : (String, scala.collection.mutable.ListBuffer[String]) = {
        val s = ",ListBuffer("
        val idx = str.indexOf(s)
        val str1 = str.substring(1,idx)
        val str2 = str.substring(idx+s.size,str.size-2)
        return (str1,str2.split(", ").to[scala.collection.mutable.ListBuffer])
      }


      LOGGER.info("Sending remaining data ...")

      var sm_final : RDD[(String, scala.collection.mutable.ListBuffer[String])] = null
      try {
        sm_final = sc.textFile(inputRemaining).map(x=>textToObj(x))
        LOGGER.info("sm_final: " + sm_final.collect.mkString("\n"))
      }
      catch{
        case e: Exception => {
          LOGGER.severe(e.getMessage)
          return -1
        }
      }

      return send_rdd_data(sm_final, outputRemaining)
    }

  }
}
