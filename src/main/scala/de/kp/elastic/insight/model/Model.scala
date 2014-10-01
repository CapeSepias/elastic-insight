package de.kp.elastic.insight.model
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

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

case class ServiceRequest(
  service:String,task:String,data:Map[String,String]
)

case class ServiceResponse(
  service:String,task:String,data:Map[String,String],status:String
)

case class Pattern(
  support:Int,itemsets:List[List[Int]])

case class Patterns(items:List[Pattern])

case class Rule (
  antecedent:List[Int],consequent:List[Int],support:Int,confidence:Double)

case class Rules(items:List[Rule])

object ResponseStatus {
  
  val FAILURE:String = "failure"
  val SUCCESS:String = "success"
    
}

object Serializer {
    
  implicit val formats = Serialization.formats(NoTypeHints)

  def serializePatterns(patterns:Patterns):String = write(patterns) 
  def deserializePatterns(patterns:String):Patterns = read[Patterns](patterns)
  
  def serializeRules(rules:Rules):String = write(rules)  
  def deserializeRules(rules:String):Rules = read[Rules](rules)
  
}
