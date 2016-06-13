require_relative "../test_helper"
require_relative "../../lib/sneakers_handlers/delay_strategies"

class SneakersHandlers::DelayStrategiesTest < Minitest::Test

  def test_exponential_strategy_works
    result = (1..10).map { |x| SneakersHandlers::DelayStrategies::EXPONENTIAL.call(x) }

    assert_equal result, [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]
  end
end
