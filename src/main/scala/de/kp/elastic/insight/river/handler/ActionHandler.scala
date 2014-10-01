package de.kp.elastic.insight.river.handler
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

import org.elasticsearch.client.Client

import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.common.settings.Settings

import org.elasticsearch.river.RiverSettings

import de.kp.elastic.insight.model._

class ActionHandler(riverSettings:RiverSettings,client:Client) {

  val globalSettings = riverSettings.globalSettings()
  val rootSettings = riverSettings.settings()
  
  val logger = Loggers.getLogger(getClass(), globalSettings)

  protected def execute(service:String) {}
    
  protected def failure(req:ServiceRequest):ServiceResponse = {
    
    val uid = req.data("uid")    
    new ServiceResponse(req.service,req.task,Map("uid" -> uid),ResponseStatus.FAILURE)	
  
  }

}