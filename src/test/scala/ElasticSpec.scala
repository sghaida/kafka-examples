import com.sghaida.elk.Elastic
import com.sksamuel.elastic4s.ElasticApi
import org.scalatest.{FlatSpec, Matchers, OptionValues}

class ElasticSpec extends FlatSpec with Matchers with OptionValues{

  implicit def toOption[T](a: T): Some[T] = Some(a)

  val es = Elastic("https://sghaida-2715151224.eu-west-1.bonsaisearch.net:443","c8ghylkhlf", "sstbbfn71v")

  "create index" should "return true" in{

    val esMapping = ElasticApi.mapping("t1").fields(
      Seq(
        ElasticApi.textField("name"),
        ElasticApi.intField("age")
      )
    )

    val res = es.createIndex("test",Some(esMapping), "t1")

    assert(res.isRight && res.right.get, "index hasn't been created")
  }

  "insert into index" should "return true" in{

    val res = es.insertDoc("test", "t1", Map("id" -> 1, "name" -> "saddam", "age" -> 38))

    assert(res.isRight, "doc hasn't been inserted")
  }

  "delete index " should "return true" in{

    val res = es.deleteIndex("test")

    assert(res, "index hasn't been deleted")
  }

}
