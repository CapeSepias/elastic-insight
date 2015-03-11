package de.kp.elastic.insight.rest

import org.elasticsearch.rest._

import org.elasticsearch.client.Client

import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.BulkResponse

import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

//import org.elasticsearch.rest.RestStatus.OK
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.index.IndexRequest.OpType

import de.kp.elastic.insight.model._

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

abstract class TrackHandler(settings:Settings,client:Client) extends BaseRestHandler(settings,client) {
  
  def executeRequest(request:RestRequest,channel:RestChannel) {
    
    try {
  
      val params = getParams(request)
      /*
       * Extract common parameters from request 
       * data and validate existence
       */        
      val service = params("service").asInstanceOf[String]    
      val subject = params("subject").asInstanceOf[String]

      if (Services.isService(service) == false) {
        throw new Exception("No <service> found.")      
      }
      
      prepareWrite(params)
      
      service match {

	    case "association" => {
	    
	      val topics = List("item")
	      if (topics.contains(subject) == false) throw new Exception("No <subject> found.")
	    
	      val source = createItemSource(params)
          writeBulk(request,channel,params,source)
          
	    }
	    case "context" => {
	    
          val topics = List("point")
	      if (topics.contains(subject) == false) throw new Exception("No <subject> found.")

          val source = createPointSource(params)
          write(request,channel,params,source) 

	    }
        case "decision" => {
	    
          val topics = List("point")
	      if (topics.contains(subject) == false) throw new Exception("No <subject> found.")

          val source = createPointSource(params)
          write(request,channel,params,source) 

        }
        case "intent" => {
	    
	      val topics = List("state")
	      if (topics.contains(subject) == false) throw new Exception("No <subject> found.")

          val source = createStateSource(params)
          write(request,channel,params,source) 
     
        }
	    case "outlier" => {
	    
	      val topics = List("state","vector")
	      if (topics.contains(subject) == false) throw new Exception("No <subject> found.")
	    
	      if (subject == "state") {

	        val source = createStateSource(params)
            write(request,channel,params,source) 
	      
	      } else {

	        val source = createVectorSource(params)
            write(request,channel,params,source) 
	      
	      }
	    
	    }
	    case "series" => {
	    
	      val topics = List("sequence")
	      if (topics.contains(subject) == false) throw new Exception("No <subject> found.")
	    
          val source = createSequenceSource(params)
          write(request,channel,params,source) 

	    }
	    case "similarity" => {
	    
	      val topics = List("sequence","vector")
	      if (topics.contains(subject) == false) throw new Exception("No <subject> found.")
	    
	      if (subject == "sequence") {

	        val source = createSequenceSource(params)
            write(request,channel,params,source) 
	      
	      } else {

	        val source = createVectorSource(params)
            write(request,channel,params,source) 

	      }
	    
	    }
	    case "social" => {
	      /* Not implemented yet */
	    }
	    case "text" => {
	      /* Not implemented yet */
	    }

	    case _ => throw new Exception("Unknown service.")
	  
      }
      
    } catch {
      case e:Throwable => sendError(channel,e)        
    }
    
  }

  private def sendResponse(request:RestRequest,channel:RestChannel,params:Map[String,Any]) {
	            
    try {
	  
      val pretty = 
        if (request.param("pretty") != null && !"false".equalsIgnoreCase(request.param("pretty"))) true else false
      
      val builder = XContentFactory.jsonBuilder()
	  if (pretty) builder.prettyPrint().lfAtEnd()

	  builder
        .startObject()
          .field("service",params("service").asInstanceOf[String])
          .field("uid",params("uid").asInstanceOf[String])
          .field("task","track")
          .field("status","success")
    
	    .endObject()
 
	  channel.sendResponse(new BytesRestResponse(RestStatus.OK,builder))
	            
    } catch {
      case e:Exception => throw new Exception("Failed to build a response.", e)
    
    }   
    
  }
  
  private def sendError(channel:RestChannel,t:Throwable) {
        
    try {
      channel.sendResponse(new BytesRestResponse(channel, t))
        
    } catch {
      case e:Throwable => logger.error("Failed to send a failure response.", e);
  
    }
    
  }

  private def getParams(request:RestRequest):Map[String,Any] = {

    val data = HashMap.empty[String,Any]
    
    /* Append request parameters */
    request.params().foreach(entry => {
      data += entry._1-> entry._2
    })
    
    /* Append content parameters */
    val params = XContentFactory.xContent(request.content()).createParser(request.content()).mapAndClose()
    params.foreach(entry => {
      data += entry._1-> entry._2
    })
      
    data.toMap

  }
  
  private def prepareWrite(params:Map[String,Any]) {
      
    val index = params("index").asInstanceOf[String]
    val mapping = params("type").asInstanceOf[String]

    val indices = client.admin().indices
    /*
     * Check whether referenced index exists; if index does not
     * exist, through exception
     */
    val existsResponse = indices.prepareExists(index).execute().actionGet()            
    if (existsResponse.isExists() == false) {
      new Exception("Index '" + index + "' does not exist.")            
    }

    /*
     * Check whether the referenced mapping exists; if mapping
     * does not exist, through exception
     */
    val prepareResponse = indices.prepareGetMappings(index).setTypes(mapping).execute().actionGet()
    if (prepareResponse.mappings().isEmpty) {
      new Exception("Mapping '" + index + "/" + mapping + "' does not exist.")
    }
    
  }
  
  private def write(request:RestRequest,channel:RestChannel,params:Map[String,Any],source:XContentBuilder) {
      
    val index = params("index").asInstanceOf[String]
    val mapping = params("type").asInstanceOf[String]
    
    /*
     * The OpType INDEX (other than CREATE) ensures that the document is
     * 'updated' which means an existing document is replaced and reindexed
     */
    client.prepareIndex(index, mapping).setSource(source).setRefresh(true).setOpType(OpType.INDEX)
      .execute(new ActionListener[IndexResponse]() {
        override def onResponse(response:IndexResponse) {
          /*
           * Registration of provided source successfully performed; no further
           * action, just logging this event
           */
          val msg = String.format("""Successful registration for: %s""", source.toString)
          logger.info(msg)

          sendResponse(request,channel,params)
         
        }      

        override def onFailure(t:Throwable) {
	      /*
	       * In case of failure, we expect one or both of the following causes:
	       * the index and / or the respective mapping may not exists
	       */
          val msg = String.format("""Failed to register %s""", source.toString)
          logger.info(msg,t)
	      
          sendError(channel,t)  
	    
        }
        
      })
  
  }
  
  private def writeBulk(request:RestRequest,channel:RestChannel,params:Map[String,Any],sources:List[XContentBuilder]) {
      
    val index = params("index").asInstanceOf[String]
    val mapping = params("type").asInstanceOf[String]
    
    /*
     * Prepare bulk request and fill with sources
     */
    val bulkRequest = client.prepareBulk()
    for (source <- sources) {
      bulkRequest.add(client.prepareIndex(index, mapping).setSource(source).setRefresh(true).setOpType(OpType.INDEX))
    }
    
    bulkRequest.execute(new ActionListener[BulkResponse](){
      override def onResponse(response:BulkResponse) {

        if (response.hasFailures()) {
          
          val msg = String.format("""Failed to register data for %s/%s""",index,mapping)
          logger.error(msg, response.buildFailureMessage())
          
          sendError(channel,new Exception(msg))
          
        } else {
          
          val msg = "Successful registration of bulk sources."
          logger.info(msg)

          sendResponse(request,channel,params)
          
        }        
      
      }
       
      override def onFailure(t:Throwable) {
	    /*
	     * In case of failure, we expect one or both of the following causes:
	     * the index and / or the respective mapping may not exists
	     */
        val msg = "Failed to register bulk of sources."
        logger.info(msg,t)
	      
        sendError(channel,t)  
	    
      }
      
    })
  
  }
  
  private def createItemSource(params:Map[String,Any]):List[XContentBuilder] = {

    /*
     * The 'item' field specifies a comma-separated list
     * of item (e.g.) product identifiers. Note, that every
     * item is actually indexed individually. This is due to
     * synergy effects with other data sources
     */
    val items = params("item").asInstanceOf[String].split(",").map(_.toInt)
    /*
     * A trackable event may have a 'score' field assigned;
     * note, that this field is optional
     */
    val scores = if (params.contains("score")) params("score").asInstanceOf[String].split(",").map(_.toDouble) else Array.fill[Double](items.length)(0)

    val zipped = items.zip(scores)
    
    /* Common field values */
    val uid = params("uid").asInstanceOf[String]

    val site = params("site").asInstanceOf[String]
    val user = params("user").asInstanceOf[String]
     
    val timestamp = params("timestamp").asInstanceOf[Long] 
    val group = params("group").asInstanceOf[String]

    zipped.map(x => {
      
      val (item,score) = x

      val builder = XContentFactory.jsonBuilder()
	  builder.startObject()
	  
	  builder.field("uid", uid)

	  builder.field("site", site)
	  builder.field("user", user)

	  builder.field("timestamp", timestamp)
	  builder.field("group", group)

	  builder.field("item", item)
	  builder.field("score", score)
	  
      builder.endObject()
	  
      builder
    
    }).toList
    
  }
  
  def createPointSource(params:Map[String,Any]):XContentBuilder = {
    
    val uid = params("uid").asInstanceOf[String]
    
    val row = params("row").asInstanceOf[Long] 
    val col = params("col").asInstanceOf[Long] 
    
    val cat = params("cat").asInstanceOf[String]
    val value = params("val").asInstanceOf[String]

    val builder = XContentFactory.jsonBuilder()
	builder.startObject()

	builder.field("uid", uid)
	  
	builder.field("row", row)
    builder.field("col", col)

    builder.field("cat", cat)
    builder.field("val", value)
	  
    builder.endObject()
    builder
    
  }
  
  def createSequenceSource(params:Map[String,Any]):XContentBuilder = {
    
    val uid = params("uid").asInstanceOf[String]
    
    val site = params("site").asInstanceOf[String]
    val user = params("user").asInstanceOf[String]
    
    val timestamp = params("timestamp").asInstanceOf[Long]
    val group = params("group").asInstanceOf[String]

    val item = params("item").asInstanceOf[Int]

    val builder = XContentFactory.jsonBuilder()
	builder.startObject()

	builder.field("uid", uid)
	  
	builder.field("site", site)
    builder.field("user", user)

    builder.field("timestamp", timestamp)
    builder.field("group", group)

    builder.field("item", item)
	  
    builder.endObject()
    builder
    
  }
  
  private def createStateSource(params:Map[String,Any]):XContentBuilder = {
    
    val uid = params("uid").asInstanceOf[String]
    
    val site = params("site").asInstanceOf[String]
    val user = params("user").asInstanceOf[String]
    
    val timestamp = params("timestamp").asInstanceOf[Long] 
    val state = params("state").asInstanceOf[String]

    val builder = XContentFactory.jsonBuilder()
	builder.startObject()

	builder.field("uid", uid)
	  
	builder.field("site", site)
    builder.field("user", user)

    builder.field("timestamp", timestamp)
    builder.field("state", state)
	  
    builder.endObject()
    builder
    
  }
  
  private def createVectorSource(params:Map[String,Any]):XContentBuilder = {
    
    val uid = params("uid").asInstanceOf[String]
    
    val row = params("row").asInstanceOf[Long] 
    val col = params("col").asInstanceOf[Long] 
   
    val label = params("lbl").asInstanceOf[String]
    val value = params("val").asInstanceOf[Double]

    val builder = XContentFactory.jsonBuilder()
	builder.startObject()

	builder.field("uid", uid)
	  
	builder.field("row", row)
    builder.field("col", col)

    builder.field("lbl", label)
    builder.field("val", value)
	  
    builder.endObject()
    builder
    
  }

}