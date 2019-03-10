import com.sghaida.elk.Elastic
import com.sksamuel.elastic4s.ElasticApi
import org.scalatest.{FlatSpec, Matchers, OptionValues}

class ElasticSpec extends FlatSpec with Matchers with OptionValues{

  implicit def toOption[T](a: T): Some[T] = Some(a)

  val es = Elastic("host:443","username", "password")

  "create index " should "return true" in{

    val res = es.createIndex("test",ElasticApi.mapping("test_mapping").fields(
      ElasticApi.textField("name")))

    assert(res, "index hasn't been created")
  }

  "delete index " should "return true" in{

    val res = es.deleteIndex("test")

    assert(res, "index hasn't been deleted")
  }

}
