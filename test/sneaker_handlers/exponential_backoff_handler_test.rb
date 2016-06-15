require_relative "../test_helper"
require_relative "../support/configurable_backoff_handler_test_base"

class SneakersHandlers::ExponentialBackoffHandlerDefaultsTest < Minitest::Test
  include ConfigurableBackoffHandlerTestBase

  queue_name 'sneakers_handlers.exponential_backoff_handler_defaults'

  expected_retries [1, 4, 9]

  handler SneakersHandlers::ExponentialBackoffHandler,
    max_retries: 3
end


class SneakersHandlers::ExponentialBackoffHandlerTest < Minitest::Test
  include ConfigurableBackoffHandlerTestBase

  queue_name 'sneakers_handlers.exponential_backoff_handler'

  expected_retries [3, 4.242, 5.196]

  handler SneakersHandlers::ExponentialBackoffHandler,
    max_retries: 3,
    exponent: 0.5,
    scale: 3
end

