import com.sghaida.twitter.TwitterJ
import org.scalatest.{FlatSpec, Matchers, OptionValues}

class TwitterJSpec extends FlatSpec with Matchers with OptionValues{

  private val consumerApiKey = sys.env.getOrElse(
    "TWITTER_CONSUMER_API_KEY", "we62Lu78d330NvrUGPLPXuWBl"
  )

  private val consumerApiSecret = sys.env.getOrElse(
    "TWITTER_CONSUMER_API_SECRET", "ScreiRYC71LwUWe4OY1yJZOsHVSOaBCuDLvnmzbZGhIuPqwX6f"
  )

  private val accessToken = sys.env.getOrElse(
    "TWITTER_ACCESS_TOKEN", "71761115-xIxcnp0inlG3BP56n9s9p5qcPm0Mwf9uLGwf8wZcR"
  )

  private val accessTokenSecret = sys.env.getOrElse(
    "TWITTER_ACCESS_TOKEN_SECRET", "RI5HDpfShAiC9pJDznCMeNVNEyKtFD768ZH4tZ48IMW5f"
  )

  val twitter = TwitterJ(
     key = consumerApiKey,
    secret = consumerApiSecret,
    accessToken = accessToken,
    tokenSecret = accessTokenSecret
  )

  "search twitter " should "return at least one tweet" in{
    val res = twitter.search("saddam")
      assert(res.head != "")
  }
}
