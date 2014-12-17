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

import de.kp.elastic.insight.io.{GetRequestBuilder,GetResponseBuilder}

class GetAction @Inject()(settings:Settings,client:Client,controller:RestController) extends InsightRestHandler(settings, client) {

  logger.info("Add GetAction module")  
  controller.registerHandler(RestRequest.Method.POST,"/_analytics/get/{service}/{subject}", this)

  private val requestBuilder  = new GetRequestBuilder()
  private val responseBuilder = new GetResponseBuilder()
  
  override protected def handleRequest(request:RestRequest,channel:RestChannel,client:Client) {

    try {

      logger.info("GetAction: Request received")
      executeRequest(request,channel,requestBuilder,responseBuilder)
      
    } catch {      
      case e:Exception => onError(channel,e)
       
    }
    
  }

}