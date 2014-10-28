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

import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings

import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.rest.RestStatus.OK

import de.kp.elastic.insight.model._
import de.kp.elastic.insight.context.AnalyticsContext

import de.kp.elastic.insight.exception.AnalyticsException
import de.kp.elastic.insight.get.{RequestBuilder,ResponseBuilder}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConversions._

class GetAction @Inject()(settings:Settings,client:Client,controller:RestController) extends BaseRestHandler(settings, client) {
  
  controller.registerHandler(RestRequest.Method.POST,"/{index}/{type}/_analytics/{service}/{uid}/{concept}", this)
  controller.registerHandler(RestRequest.Method.POST,"/{index}/_analytics/get/{service}/{uid}/{concept}", this)

  override protected def handleRequest(request:RestRequest,channel:RestChannel,client:Client) {

    try {
  
      val params = XContentFactory.xContent(request.content()).createParser(request.content()).mapAndClose().toMap
      
      val req = RequestBuilder.build(params)
      val response = AnalyticsContext.send(req).mapTo[ServiceResponse]
      
      response.onSuccess {
        case result => onResponse(channel,request,result)
      }
    
      response.onFailure {
        case throwable => onError(channel,throwable)
	  }
      
    } catch {
      
      case e:Exception => onError(channel,e)
       
    }
    
  }

  private def onResponse(channel:RestChannel,request:RestRequest,response:ServiceResponse) {
	            
    try {
	  
      val pretty = 
        if (request.param("pretty") != null && !"false".equalsIgnoreCase(request.param("pretty"))) true else false
	  
      val builder = ResponseBuilder.build(response,pretty)
	  channel.sendResponse(new BytesRestResponse(RestStatus.OK,builder))
	            
    } catch {
      case e:IOException => throw new AnalyticsException("Failed to build a response.", e)
    
    }   
    
  }
 
  private def onError(channel:RestChannel,t:Throwable) {
        
    try {
      channel.sendResponse(new BytesRestResponse(channel, t))
        
    } catch {
      case e:Throwable => logger.error("Failed to send a failure response.", e);
  
    }
    
  }

}