# Using this handler, failed messages will be retried with an exponential
# backoff delay, for a certain number of times, until they are dead-lettered.
#
# The specific strategy was adapted from the following sources:
#   https://en.wikipedia.org/wiki/Exponential_backoff
#   https://alexandrebrisebois.wordpress.com/2013/02/19/calculating-an-exponential-back-off-delay-based-on-failed-attempts/
#   https://gist.github.com/osmanehmad/1640033
#
# It results in the following values:
#  _________________________________________________________
#  |               Delay for attempt |  Total elapsed time |
#  |---------------------------------+---------------------|
#  | Attempt  1:                0.5s |                0.5s |
#  | Attempt  2:                1.5s |                2.0s |
#  | Attempt  3:                3.5s |                5.5s |
#  | Attempt  4:                7.5s |               13.0s |
#  | Attempt  5:               15.5s |               28.5s |
#  | Attempt  6:               31.5s |            1m  0.0s |
#  |                                 |                     |
#  | Attempt  7:            1m  3.5s |            2m  3.5s |
#  | Attempt  8:            2m  7.5s |            4m 11.0s |
#  | Attempt  9:            4m 15.5s |            8m 26.5s |
#  | Attempt 10:            8m 31.5s |           16m 58.0s |
#  | Attempt 11:           17m  3.5s |           34m  1.5s |
#  | Attempt 12:           34m  7.5s |        1h  8m  9.0s |
#  |                                 |                     |
#  | Attempt 13:        1h  8m 15.5s |        2h 16m 24.5s |
#  | Attempt 14:        2h 16m 31.5s |        4h 32m 56.0s |
#  | Attempt 15:        4h 33m  3.5s |        9h  5m 59.5s |
#  | Attempt 16:        9h  6m  7.5s |       18h 12m  7.0s |
#  | Attempt 17:       18h 12m 15.5s |    1d 12h 24m 22.5s |
#  |                                 |                     |
#  | Attempt 18:    1d 12h 24m 31.5s |    3d     48m 54.0s |
#  | Attempt 19:    3d     49m  3.5s |    6d  1h 37m 57.5s |
#  | Attempt 20:    6d  1h 38m  7.5s |   12d  3h 16m  5.0s |
#  | Attempt 21:   12d  3h 16m 15.5s |   24d  6h 32m 20.5s |
#  | Attempt 22:   24d  6h 32m 31.5s |   48d 13h  4m 52.0s |
#  | Attempt 23:   48d 13h  5m  3.5s |   97d  2h  9m 55.5s |
#  | Attempt 24:   97d  2h 10m  7.5s |  194d  4h 20m  3.0s |
#  | Attempt 25:  194d  4h 20m 15.5s |  388d  8h 40m 18.5s |
#  ---------------------------------------------------------
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
