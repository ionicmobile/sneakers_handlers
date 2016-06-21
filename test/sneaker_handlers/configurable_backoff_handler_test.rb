require_relative "../test_helper"
require_relative "../support/configurable_backoff_handler_test_base"

class SneakersHandlers::ConfigurableBackoffHandlerTest < Minitest::Test
  include ConfigurableBackoffHandlerTestBase

  expected_retries [0.5, 1.0, 1.5]

  handler SneakersHandlers::ConfigurableBackoffHandler,
    max_retries: 3,
    delay_strategy: lambda { |x| x / 2.0 }
end
