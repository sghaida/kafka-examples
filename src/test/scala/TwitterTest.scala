import com.sghaida.twitter.Twitter
import org.scalatest.{FlatSpec, Matchers, OptionValues}

class TwitterTest extends FlatSpec with Matchers with OptionValues{

  private val consumerApiKey = sys.env.getOrElse(
    "TWITTER_CONSUMER_API_KEY", throw new Exception("TWITTER_CONSUMER_API_KEY is not defined in env")
  )

  private val consumerApiSecret = sys.env.getOrElse(
    "TWITTER_CONSUMER_API_SECRET", throw new Exception("TWITTER_CONSUMER_API_SECRET is not defined in env")
  )

  private val accessToken = sys.env.getOrElse(
    "TWITTER_ACCESS_TOKEN", throw new Exception("TWITTER_ACCESS_TOKEN is not defined in env")
  )

  private val accessTokenSecret = sys.env.getOrElse(
    "TWITTER_ACCESS_TOKEN_SECRET", throw new Exception("TWITTER_ACCESS_TOKEN_SECRET is not defined in env")
  )

  val twitter = Twitter(
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
