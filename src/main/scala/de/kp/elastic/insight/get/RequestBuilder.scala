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

  def build(params:Map[String,Any]):ServiceRequest = {
        
    val service = params("service").asInstanceOf[String]
    if (Services.isService(service) == false) {
      throw new AnalyticsException("No <service> found.")
      
    }

    val concept = params("concept").asInstanceOf[String]
    if (Concepts.isConcept(concept) == false) {
      throw new AnalyticsException("No <concept> found.")
    }
    
    val task = "get:" + concept
    
    /* Build request data */
    val data = HashMap.empty[String,String]
    
    data += "uid" -> params("uid").asInstanceOf[String]
   
    service match {
      
      case Services.ASSOCIATION => {
        /*
         * Association analysis requires input parameters for 'follow' requests;
         * the input either specifies antecedents for consequent retrieval or
         * vice versa.
         */
        concept match {
          
          case Concepts.FOLLOWERS => {
 
            if (params.contains("antecedent") == false && params.contains("consequent") == false) {
              throw new AnalyticsException("Not enough parameters to retrieve followers.")              
            }
            
            if (params.contains("antecedent")) {
              
              val antecedent = params("antecedent").asInstanceOf[List[Int]]
              data += "antecedent" -> antecedent.mkString(",")
              
            } else {
              
              val consequent = params("consequent").asInstanceOf[List[Int]]
              data += "consequent" -> consequent.mkString(",")
              
            }
            
          }
          
        }
         
      }
      case Services.CONTEXT => {
         
        if (params.contains("features") == false) {
          throw new AnalyticsException("Not enough parameters to retrieve predictions.")                        
        }
        
        val features = params("features").asInstanceOf[List[String]]
        data += "features" -> features.mkString(",")
       
      }
      case Services.DECISION => {
        
        if (params.contains("features") == false) {
          throw new AnalyticsException("Not enough parameters to retrieve predictions.")                        
        }
        
        val features = params("features").asInstanceOf[List[String]]
        data += "features" -> features.mkString(",")

      }
      case Services.INTENT => {
        /*
         * Intent recognition does not require any input parameters; the analysis
         * is performed on the data provided by the respective data sources
         */
      }
      case Services.OUTLIER => {
        /*
         * Outlier detection does not require any input parameters; the analysis
         * is performed on the data provided by the respective data sources
         */
      }
      case Services.SERIES => {
        /*
         * Series analysis requires input parameters for 'follow' requests;
         * the input either specifies antecedents for consequent retrieval 
         * or vice versa.
         */
        concept match {
          
          case Concepts.FOLLOWERS => {
 
            if (params.contains("antecedent") == false && params.contains("consequent") == false) {
              throw new AnalyticsException("Not enough parameters to retrieve followers.")              
            }
            
            if (params.contains("antecedent")) {
              
              val antecedent = params("antecedent").asInstanceOf[List[Int]]
              data += "antecedent" -> antecedent.mkString(",")
              
            } else {
              
              val consequent = params("consequent").asInstanceOf[List[Int]]
              data += "consequent" -> consequent.mkString(",")
              
            }
            
          }
          
        }
        
      }
      case Services.SIMILARITY => {
        /*
         * Similarity analysis does not require any input parameters; the analysis
         * is performed on the data provided by the respective data sources
         */
      }
      case Services.SOCIAL => {
        /* not yet implemented */
      }
      case Services.TEXT => {
         /* not yet implemented */       
      }
    
    }
    
    new ServiceRequest(service, task, data.toMap)

  }

}
