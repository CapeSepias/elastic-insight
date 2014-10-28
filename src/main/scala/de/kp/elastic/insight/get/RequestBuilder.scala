package de.kp.elastic.insight.get
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

import de.kp.elastic.insight.model._
import de.kp.elastic.insight.exception.AnalyticsException

import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

object RequestBuilder {

  def build(params:java.util.Map[String,String]):ServiceRequest = {
        
    val service = params("service")
    if (Services.isService(service) == false) {
      throw new AnalyticsException("No <service> found.")
      
    }

    val concept = params("concept")
    if (Concepts.isConcept(concept) == false) {
      throw new AnalyticsException("No <concept> found.")
    }
    
    val task = "get:" + concept
    
    /* Build request data */
    val data = HashMap.empty[String,String]
    
    data += "uid" -> params("uid")
   
    // TODO
    
    null

  }

}
