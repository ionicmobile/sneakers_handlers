class RetryWorkerSuccess
  include Sneakers::Worker

  from_queue "sneaker_handlers.retry_success_test",
  ack: true,
  durable: false,
  exchange: "sneakers_handlers",
  exchange_type: :topic,
  routing_key: "sneakers_handlers.retry_test",
  handler: SneakersHandlers::RetryHandler,
  arguments: { "x-dead-letter-exchange" => "sneakers_handlers.dlx",
               "x-dead-letter-routing-key" => "sneaker_handlers.retry_success_test" }

  def work(payload)
    return ack!
  end
end
