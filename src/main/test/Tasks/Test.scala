import com.typesafe.config.{Config, ConfigFactory}

class Test extends AnyFlatSpec with Matchers {

  behavior of "configuration parameters module"
  val config: Config = ConfigFactory.load("application.conf").getConfig("TaskGenerator")


  it should "ragex pattern" in {
    config.getString("regex_pattern") should be "(.*?)"
  }
  it should "time interval" in {
    config.getInt("time_interval") should be 1
  }


}