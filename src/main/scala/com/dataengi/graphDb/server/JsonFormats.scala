package com.dataengi.graphDb.server

import com.dataengi.graphDb.actors.Node
import com.dataengi.graphDb.actors.Node.Fields
import com.dataengi.graphDb.dsl.Graph._
import spray.json.DefaultJsonProtocol._
import spray.json.{
  JsBoolean,
  JsNumber,
  JsObject,
  JsString,
  JsValue,
  RootJsonFormat
}

object JsonFormats {

  implicit val updateFieldFormat: RootJsonFormat[Routes.UpdateField] =
    jsonFormat3(Routes.UpdateField)
  implicit val addNodeFormat: RootJsonFormat[Routes.AddNode] =
    jsonFormat2(Routes.AddNode)
  implicit val removeFieldFormat: RootJsonFormat[Routes.RemoveField] =
    jsonFormat1(Routes.RemoveField)
  implicit val updateOneToOneFormat: RootJsonFormat[Routes.UpdateOneToOneRel] =
    jsonFormat2(Routes.UpdateOneToOneRel)
  implicit val removeRelationFormat: RootJsonFormat[Routes.RemoveRelationBody] =
    jsonFormat1(Routes.RemoveRelationBody)
  implicit object FieldsJsonFormat extends RootJsonFormat[Fields] {
    def write(value: Fields): JsObject =
      JsObject(value.view.mapValues {
        case StringValue(value) => JsString(value)
        case NumberValue(value) => JsNumber(value)
        case BoolValue(value)   => JsBoolean(value)
      }.toMap)
    def read(value: JsValue): Fields = ???
  }

  implicit val StateFormat: RootJsonFormat[Node.NodeState] = jsonFormat4(
    Node.NodeState
  )
}
