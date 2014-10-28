package de.kp.elastic.insight.rest
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Elastic-Insight project
* (https://github.com/skrusche63/elastic-insight).
* 
* Elastic-Insight is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Elastic-Insight is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Elastic-Insight. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import org.elasticsearch.rest._
import org.elasticsearch.client.Client

import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings

import org.elasticsearch.common.xcontent.ToXContent.Params
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.json.JsonXContent

import org.elasticsearch.rest.RestStatus.OK

import de.kp.elastic.insight.exception.AnalyticsException

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

class TrackAction @Inject()(settings:Settings,client:Client,controller:RestController) extends BaseRestHandler(settings,client) {

  /* Registration of the URL part that is responsible for indexing data */
  controller.registerHandler(RestRequest.Method.POST,"/{index}/{type}/_analytics/track/{topic}", this)
  controller.registerHandler(RestRequest.Method.POST,"/{index}/_analytics/track/{topic}", this)
  
  override protected def handleRequest(request:RestRequest,channel:RestChannel,client:Client) {

    try {

      val topic = request.param("topic")      
      topic match {

        /**
         * The amount data structure is based on the RFM model: 
         * R(ecency), F(requency) and M(onetary value) and is an 
         * appropriate starting point for intent recognition
         */
        case "amount" => registerRecord(request,channel,client,topic)
        
        /**
         * The extended item data structure is common to outlier
         * detection
         */
        case "extended_item" => registerRecord(request,channel,client,topic)
        
        /**
         * The item data structure is common to association, series
         * and similarity analysis
         */        
        case "item" => registerRecord(request,channel,client,topic)
        
        /**
         * The decision feature data structure is common to decision
         * analysis
         */        
        case "decision_feature" => registerFeature(request,channel,client,topic)
        
        /**
         * The labeled feature data structure is common to outlier
         * detection and similarity analysis
         */
        case "labeled_feature" => registerFeature(request,channel,client,topic)
        
        /**
         * The targeted feature data structure is common to context-aware
         * analysis
         */
        case "targeted_feature" => registerFeature(request,channel,client,topic)
        
        case _ => {
          onError(channel, new AnalyticsException("No <topic> found."))
          return    

        }

      }
        
    } catch {
      case e:Exception => createOnErrorListener(channel).onError(e)       
    }
    
  }
  
  private def registerFeature(request:RestRequest,channel:RestChannel,client:Client,topic:String) {

    val requestMap = XContentFactory.xContent(request.content()).createParser(request.content()).mapAndClose().toMap
    val paramMap = HashMap.empty[String,Any]
  
    val featureHandler = new FeatureRequestHandler(settings,client,topic)
        
    val chain = new RequestHandlerChain(Array[RequestHandler](featureHandler,createAcknowledgedHandler(request,channel)))
    chain.execute(request, createOnErrorListener(channel), requestMap, paramMap)
  
  }
   
  private def registerRecord(request:RestRequest,channel:RestChannel,client:Client,topic:String) {

    val requestMap = XContentFactory.xContent(request.content()).createParser(request.content()).mapAndClose().toMap
    val paramMap = HashMap.empty[String,Any]

    val recordHandler  = new RecordRequestHandler(settings,client,topic)  
            
    val chain = new RequestHandlerChain(Array[RequestHandler](recordHandler,createAcknowledgedHandler(request,channel)))
    chain.execute(request, createOnErrorListener(channel), requestMap, paramMap)
        
  }
  
  private def createAcknowledgedHandler(request:RestRequest,channel:RestChannel):RequestHandler = {
    	
    return new RequestHandler() {

	  override def execute(params:Params,listener:OnErrorListener,requestMap:Map[String,Any],paramMap:HashMap[String,Any],chain:RequestHandlerChain) {

		try {
	      
		  val builder = JsonXContent.contentBuilder()
	      val pretty = request.param("pretty")
	                
	      if (pretty != null && !"false".equalsIgnoreCase(pretty)) {
	         builder.prettyPrint().lfAtEnd()
	      }
	                
	      builder.startObject()
	      builder.field("acknowledged", true)
	                
	      builder.endObject()
	      channel.sendResponse(new BytesRestResponse(OK, builder))
	            
		} catch {
		    
		  case e:Exception => {    
		    try {
	            channel.sendResponse(new BytesRestResponse(channel, e))
		    } catch {
	            case ex:Exception =>  logger.error("Failed to send a failure response.", ex)
		    }
		  
		  }
		}				
	  }
    	
    }
    
  }    
  
  private def onError(channel:RestChannel,t:Throwable) {
        
    try {
      channel.sendResponse(new BytesRestResponse(channel, t))
        
    } catch {
      case e:Throwable => logger.error("Failed to send a failure response.", e);
  
    }
    
  }
  
  private def createOnErrorListener(channel:RestChannel):OnErrorListener = {
        
    return new OnErrorListener() {
			
	  override def onError(t:Throwable) {
	            
	    try {
	      channel.sendResponse(new BytesRestResponse(channel,t))
	            
	    } catch {
	        case e:Exception => logger.error("Failed to send a failure response.", e)
	            
	    }
		
	  }

    }
    
  }

}