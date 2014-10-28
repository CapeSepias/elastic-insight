package de.kp.elastic.insight.track
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

import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

class ExtendedItemBuilder {

  import de.kp.elastic.insight.track.ElasticBuilderFactory._
  
  def createBuilder(mapping:String):XContentBuilder = {
    /*
     * Define mapping schema for index 'index' and 'type'
     */
    val builder = XContentFactory.jsonBuilder()
                      .startObject()
                      .startObject(mapping)
                        .startObject("properties")

                          /* timestamp */
                          .startObject(TIMESTAMP_FIELD)
                            .field("type", "long")
                          .endObject()
                    
                          /* site */
                          .startObject(SITE_FIELD)
                            .field("type", "string")
                            .field("index", "not_analyzed")
                          .endObject()

                          /* user */
                          .startObject(USER_FIELD)
                            .field("type", "string")
                            .field("index", "not_analyzed")
                          .endObject()//

                          /* group */
                          .startObject(GROUP_FIELD)
                            .field("type", "string")
                            .field("index", "not_analyzed")
                          .endObject()//

                          /* item */
                          .startObject(ITEM_FIELD)
                            .field("type", "integer")
                          .endObject()
                          
                          /* price */
                          .startObject(PRICE_FIELD)
                            .field("type", "float")
                          .endObject()

                        .endObject() // properties
                      .endObject()   // mapping
                    .endObject()
                    
    builder

  }
  
  def prepare(params:Map[String,Any]):java.util.Map[String,Object] = {
     
    val source = new java.util.HashMap[String,Object]

    source.put(SITE_FIELD,params(SITE_FIELD).asInstanceOf[Object])
    source.put(USER_FIELD,params(USER_FIELD).asInstanceOf[Object])
      
    source.put(TIMESTAMP_FIELD,params(TIMESTAMP_FIELD).asInstanceOf[Object]) 

    source.put(GROUP_FIELD,params(GROUP_FIELD).asInstanceOf[Object]) 
    source.put(ITEM_FIELD,params(ITEM_FIELD).asInstanceOf[Object]) 

    source.put(PRICE_FIELD,params(PRICE_FIELD).asInstanceOf[Object]) 

    source
    
  }

}