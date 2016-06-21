require_relative "../test_helper"
require_relative "../support/configurable_backoff_handler_test_base"

class SneakersHandlers::ConventionalExponentialBackoffHandlerTest < Minitest::Test
  include ConfigurableBackoffHandlerTestBase

  queue_name 'sneakers_handlers.conventional_exponential_backoff_handler'

  expected_retries [0.5, 1.5, 3.5, 7.5]

  handler SneakersHandlers::ConventionalExponentialBackoffHandler,
          max_retries: 4
end
