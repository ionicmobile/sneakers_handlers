# Using this handler, failed messages will be retried with an exponential
# backoff delay, for a certain number of times, until they are dead-lettered.
#
# To use it you need to defined this handler in your worker:
#
# from_queue "my-app.queue_name",
#   exchange: "my_exchange_name",
#   routing_key: "my_routing_key",
#   handler: SneakersHandlers::ExponentialBackoffHandler,
#   arguments: { "x-dead-letter-exchange" => "my_exchange_name.dlx",
#                "x-dead-letter-routing-key" => "my-app.queue_name" }}
#
# By default it will retry 25 times before dead-lettering a message, but you can
# also customize that with the `max_retries` option:
#
# from_queue "my-app.queue_name",
#   exchange: "my_exchange_name",
#   routing_key: "my_routing_key",
#   max_retries: 10,
#   handler: SneakersHandlers::ExponentialBackoffHandler,
#   arguments: { "x-dead-letter-exchange" => "my_exchange_name.dlx",
#                "x-dead-letter-routing-key" => "my-app.queue_name" }}

module SneakersHandlers
  class ExponentialBackoffHandler < ConfigurableBackoffHandler

    def initialize(channel, queue, options)
      exponent = options[:exponent] || 2
      scale = options[:scale] || 1
      super(channel, queue, options.merge(delay_strategy: -> x { scale * x ** exponent }))
    end
  end
end
