package de.kp.elastic.insight.io
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

import org.elasticsearch.common.xcontent.{XContentFactory,XContentBuilder}

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.elastic.insight.model._

class FieldResponseBuilder extends ResponseBuilder {

  override def build(res:ServiceResponse,pretty:Boolean):XContentBuilder = {
     
    val builder = XContentFactory.jsonBuilder()
	if (pretty) builder.prettyPrint().lfAtEnd()

	/*
	 * Build header of response object, which contains always the same
	 * components
	 */
	builder
      .startObject()
        .field("service",res.service)
        .field("task",res.task)
        .field("status",res.status)
        .field("uid",res.data(Names.REQ_UID))
    
    /*
     * Determine whether the request has been successfull or not;
     * in case of a failure, try to incorporate the response
     * message
     */    
    if (res.status == ResponseStatus.FAILURE) {
      
      if (res.data.contains(Names.REQ_MESSAGE)) {
        builder.field("message",res.data(Names.REQ_MESSAGE))
      }
      
    } else {
      
      val fields = Serializer.deserializeFields(res.data(Names.REQ_RESPONSE)).items

      builder.startObject("result") /* begin result */
	  builder.field("total", fields.size)
	  
	  builder.startArray("data")    
      for (field <- fields) {
      
        builder.startObject()
        /* name */
        builder.field("name",field.name)
        /* datatype */
        builder.field("datatype",field.datatype)
        /* value */
        builder.field("value",field.value)      
        builder.endObject()
      
      }
	  
      builder.endArray()	  
	  builder.endObject() /* end result */
      
    }

    builder.endObject()
    builder
    
  }

}