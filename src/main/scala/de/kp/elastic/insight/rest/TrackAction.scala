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

  /*
   * Registration of the URL part that is responsible for indexing data
   * that form a transaction or sequence database
   */
  controller.registerHandler(RestRequest.Method.POST,"/{index}/{type}/_analytics/event", this)
  controller.registerHandler(RestRequest.Method.POST,"/{index}/_analytics/event", this)
  
  private val eventHandler = new EventRequestHandler(settings,client)
  
  /*
   * Registration of the URL part that is responsible for indexing data
   * that form a feature database
   */
  controller.registerHandler(RestRequest.Method.POST,"/{index}/_analytics/feature", this)
  controller.registerHandler(RestRequest.Method.POST,"/{index}/{type}/_analytics/feature", this)
  
  private val featureHandler = new FeatureRequestHandler(settings,client)
  
  override protected def handleRequest(request:RestRequest,channel:RestChannel,client:Client) {

    try {

      val requestMap = XContentFactory.xContent(request.content()).createParser(request.content()).mapAndClose().toMap
      val paramMap = HashMap.empty[String,Any]

      val hasEvent = requestMap.contains("event")
      val hasFeature = requestMap.contains("feature")

      if (hasEvent) {
            
        val chain = new RequestHandlerChain(Array[RequestHandler](eventHandler,createAcknowledgedHandler(request,channel)))
        chain.execute(request, createOnErrorListener(channel), requestMap, paramMap)
            
      } else if (hasFeature) {
        
        val chain = new RequestHandlerChain(Array[RequestHandler](featureHandler,createAcknowledgedHandler(request,channel)))
        chain.execute(request, createOnErrorListener(channel), requestMap, paramMap)
      
      } else {
        throw new AnalyticsException("No event or feature provided.")            
      }
        
    } catch {
      case e:Exception => createOnErrorListener(channel).onError(e)       
    }
    
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