package janrain.jedi.logging

import spray.json.JsObject

package object poc {
  implicit class JsObjectHelpers(json: JsObject) {
    import spray.json.DefaultJsonProtocol._
    def has(fields: String*): Boolean = fields forall json.fields.contains
    def string(key: String) = json.fields(key).convertTo[String]
  }
}
