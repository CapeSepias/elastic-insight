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

import java.io.IOException

import org.elasticsearch.rest._

import org.elasticsearch.client.Client

import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentFactory

import org.elasticsearch.rest.RestStatus.OK

import de.kp.elastic.insight.model._
import de.kp.elastic.insight.context.AnalyticsContext

import de.kp.elastic.insight.io.{RequestBuilder,ResponseBuilder}
import de.kp.elastic.insight.exception.AnalyticsException

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

import scala.concurrent.ExecutionContext.Implicits.global

abstract class InsightRestHandler(settings:Settings,client:Client) extends BaseRestHandler(settings, client) {

  protected def executeRequest(request:RestRequest,channel:RestChannel,requestBuilder:RequestBuilder,responseBuilder:ResponseBuilder) {
  
    val params = getParams(request)
    
    /*
     * Build service request and send to remote service
     */
    val req = requestBuilder.build(params)
      
    val service = req.service
    val message = Serializer.serializeRequest(req)
      
    val response = AnalyticsContext.send(service,message).mapTo[String]      
    response.onSuccess {
        case result => onResponse(channel,responseBuilder,request,Serializer.deserializeResponse(result))
    }
    
    response.onFailure {
      case throwable => onError(channel,throwable)
	}
    
  }

  protected def getParams(request:RestRequest):Map[String,Any] = {

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
 
  protected def onError(channel:RestChannel,t:Throwable) {
        
    try {
      channel.sendResponse(new BytesRestResponse(channel, t))
        
    } catch {
      case e:Throwable => logger.error("Failed to send a failure response.", e);
  
    }
    
  }
  
  protected def onResponse(channel:RestChannel,builder:ResponseBuilder,request:RestRequest,response:ServiceResponse) {
	            
    try {
	  
      val pretty = 
        if (request.param("pretty") != null && !"false".equalsIgnoreCase(request.param("pretty"))) true else false
	  
      val contentBuilder = builder.build(response,pretty)
	  channel.sendResponse(new BytesRestResponse(RestStatus.OK,contentBuilder))
	            
    } catch {
      case e:IOException => throw new AnalyticsException("Failed to build a response.", e)
    
    }   
    
  }

}