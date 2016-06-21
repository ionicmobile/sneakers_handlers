# Using this handler, failed messages will be retried with an exponential
# backoff delay, for a certain number of times, until they are dead-lettered.
#
# The specific strategy was adapted from the following sources:
#   https://en.wikipedia.org/wiki/Exponential_backoff
#   https://alexandrebrisebois.wordpress.com/2013/02/19/calculating-an-exponential-back-off-delay-based-on-failed-attempts/
#   https://gist.github.com/osmanehmad/1640033
#
# It results in the following values:
#   Attempt  1:                0.5s
#   Attempt  2:                1.5s
#   Attempt  3:                3.5s
#   Attempt  4:                7.5s
#   Attempt  5:               15.5s
#   Attempt  6:               31.5s
#
#   Attempt  7:            1m  3.5s
#   Attempt  8:            2m  7.5s
#   Attempt  9:            4m 15.5s
#   Attempt 10:            8m 31.5s
#   Attempt 11:           17m  3.5s
#   Attempt 12:           34m  7.5s
#
#   Attempt 13:        1h  8m 15.5s
#   Attempt 14:        2h 16m 31.5s
#   Attempt 15:        4h 33m  3.5s
#   Attempt 16:        9h  6m  7.5s
#   Attempt 17:       18h 12m 15.5s
#
#   Attempt 18:    1d 36h 24m 31.5s
#   Attempt 19:    3d 12h 49m  3.5s
#   Attempt 20:    6d 25h 38m  7.5s
#   Attempt 21:   12d 51h 16m 15.5s
#   Attempt 22:   24d 42h 32m 31.5s
#   Attempt 23:   48d 25h  5m  3.5s
#   Attempt 24:   97d 50h 10m  7.5s
#   Attempt 25:  194d 40h 20m 15.5s
#
# To use it you need to defined this handler in your worker:
#
# from_queue "my-app.queue_name",
#   exchange: "my_exchange_name",
#   routing_key: "my_routing_key",
#   handler: SneakersHandlers::ConventionalExponentialBackoffHandler,
#   arguments: { "x-dead-letter-exchange" => "my_exchange_name.dlx",
#                "x-dead-letter-routing-key" => "my-app.queue_name" }}
#
# The following defaults can be overriden:
#   max_retries: 25     # The number of times it retries before dead-lettering a message.
#
# from_queue "my-app.queue_name",
#   exchange: "my_exchange_name",
#   routing_key: "my_routing_key",
#   max_retries: 10,
#   handler: SneakersHandlers::ConventionalExponentialBackoffHandler,
#   arguments: { "x-dead-letter-exchange" => "my_exchange_name.dlx",
#                "x-dead-letter-routing-key" => "my-app.queue_name" }}

module SneakersHandlers
  class ConventionalExponentialBackoffHandler < ConfigurableBackoffHandler
    def initialize(channel, queue, options)
      super(channel, queue, options.merge(delay_strategy: -> (x) { (2**x - 1) / 2.0 }))
    end
  end
end
