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
import de.kp.elastic.insight.utils.StringUtils

import scala.concurrent.ExecutionContext.Implicits.global
/**
 * This action retrieves the status of a certain data mining
 * or model building task from the remote analytics service;
 * the status is held in a Redis instance and is not part of
 * the search index
 */
class StatusAction @Inject()(settings:Settings,client:Client,controller:RestController) extends InsightRestHandler(settings,client) {

  logger.info("Add StatusAction module")
  controller.registerHandler(RestRequest.Method.GET,"/_analytics/status/{service}/{uid}", this)
 
  override protected def handleRequest(request:RestRequest,channel:RestChannel,client:Client) {

    val service = request.param("service")
    if (StringUtils.isBlank(service)) {

      onError(channel, new AnalyticsException("No <service> found."))
      return
    
    }

    val uid = request.param("uid")
    if (StringUtils.isBlank(uid)) {

      onError(channel, new AnalyticsException("No <uid> found."))
      return
    
    }
    
    val req = new ServiceRequest(service,"status",Map("uid" -> uid))
    val message = Serializer.serializeRequest(req)
      
    val response = AnalyticsContext.send(service,message).mapTo[String]      
    response.onSuccess {
      case result => onResponse(channel,request,Serializer.deserializeResponse(result))
    }
    
    response.onFailure {
      case throwable => onError(channel,throwable)
	}
    
  }

  private def onResponse(channel:RestChannel,request:RestRequest,response:ServiceResponse) {
	            
    try {
	  
      val builder = XContentFactory.jsonBuilder()
	  
      val pretty = request.param("pretty")
	  if (pretty != null && !"false".equalsIgnoreCase(pretty)) {
	    builder.prettyPrint().lfAtEnd()
	  }
	  
      builder
        .startObject()
          .field("service",response.service)
          .field("task",response.status)
          .field("uid",response.data("uid"))
          .field("status",response.status)
	    .endObject()

	  channel.sendResponse(new BytesRestResponse(RestStatus.OK,builder))
	            
    } catch {
      case e:IOException => throw new AnalyticsException("Failed to build a response.", e)
    }   
    
  }

}