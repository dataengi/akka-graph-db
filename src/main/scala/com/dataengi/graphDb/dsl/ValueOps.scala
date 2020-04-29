package com.dataengi.graphDb.dsl

trait ValueOps {
  import Graph._

  // TypeClass for conversion into Value
  trait ValueAlike[T] {
    type V <: Value

    def toValue(x: T): V
  }

  trait ValueAlikeSeq[T] extends ValueAlike[Seq[T]] {
    type V = ListValue
  }

}

// TypeClass instances for supported types
object ValueInstances {
  import Graph._

  implicit val stringValueInstance = new ValueAlike[String] {
    override type V = StringValue

    override def toValue(x: String) = StringValue(x)
  }

  implicit val boolValueInstance = new ValueAlike[Boolean] {
    override type V = BoolValue

    override def toValue(x: Boolean) = BoolValue(x)
  }

  implicit val numberValueInstance = new ValueAlike[Double] {
    override type V = NumberValue

    override def toValue(x: Double) = NumberValue(x)
  }
  implicit val numberIntValueInstance = new ValueAlike[Int] {
    override type V = NumberValue

    override def toValue(x: Int) = NumberValue(x)
  }

  implicit def valueValueInstance[VV <: Value] = new ValueAlike[VV] {
    override type V = VV

    override def toValue(x: VV): V = x
  }

  implicit def valuesInstances[T: ValueAlike]: ValueAlikeSeq[T] =
    new ValueAlikeSeq[T] {
      override type V = ListValue

      override def toValue(values: Seq[T]): ListValue = {
        val al = implicitly[ValueAlike[T]]
        ListValue(values.map(al.toValue).toSet)
      }
    }

  def valueToString(value: Value): String = value match {
    case StringValue(v) => "string:" + v
    case NumberValue(v) => "number:" + v.toString
    case BoolValue(v)   => "boolean:" + v.toString
    case _              => ""
  }

  def stringToValue(string: String): Value =
    string.split(":", 2) match {
      case Array("string", value: String)  => StringValue(value)
      case Array("number", value: String) => NumberValue(value.toDouble)
      case Array("boolean", value: String) => BoolValue(value.toBoolean)
      case _                               => EmptyValue()
    }
}
