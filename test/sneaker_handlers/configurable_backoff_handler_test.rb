require_relative "../test_helper"
require_relative "../../lib/sneakers_handlers/configurable_backoff_handler"

class SneakersHandlers::ConfigurableBackoffHandlerTest < Minitest::Test
  class FailingWorker
    include Sneakers::Worker

    DELAY_VALUES = Array.new(2) { rand(1..5) }.freeze

    from_queue "sneaker_handlers.configurable_back_test",
      ack: true,
      durable: false,
      max_retries: 2,
      exchange: "sneakers_handlers",
      exchange_type: :topic,
      routing_key: ["lifecycle.created", "lifecycle.updated"],
      delay_strategy: lambda { |x| DELAY_VALUES[x] },
      handler: SneakersHandlers::ConfigurableBackoffHandler,
      arguments: {
        "x-dead-letter-exchange" => "sneakers_handlers.error",
        "x-dead-letter-routing-key" => "sneakers_handlers.configurable_back_test"
      }

    def work(payload)
      return payload.to_sym
    end
  end

  def setup
    cleanup!
  end

  def teardown
    cleanup!
  end

  def test_handler_retries_with_ttl_retry_queues
    exchange = channel.topic("sneakers_handlers", durable: false)

    ["timeout", "error", "requeue", "reject"].each do |type_of_failure|
      FailingWorker.new.run
      exchange.publish(type_of_failure, routing_key: "lifecycle.created")
      sleep 0.1
    end

    assert_equal 4, retry_queue(1).message_count
    assert_equal 0, retry_queue(2).message_count
    assert_equal 0, error_queue.message_count

    wait_for_retry 1

    assert_equal 0, retry_queue(1).message_count
    assert_equal 4, retry_queue(2).message_count
    assert_equal 0, error_queue.message_count

    wait_for_retry 2

    assert_equal 0, retry_queue(2).message_count
    assert_equal 4, error_queue.message_count

    ["timeout", "error", "requeue", "reject"].each do |type_of_failure|
      FailingWorker.new.run
      exchange.publish(type_of_failure, routing_key: "lifecycle.created")
      sleep 0.1
    end

    assert_equal 4, retry_queue(1).message_count
    assert_equal 0, retry_queue(2).message_count

    wait_for_retry 1

    assert_equal 0, retry_queue(1).message_count
    assert_equal 4, retry_queue(2).message_count

    wait_for_retry 2

    assert_equal 8, error_queue.message_count
  end

  def test_works_when_shoveling_messages
    exchange = channel.default_exchange

    ["timeout", "error", "requeue", "reject"].each do |type_of_failure|
      FailingWorker.new.run
      exchange.publish(type_of_failure, routing_key: "sneaker_handlers.configurable_back_test")
      sleep 0.1
    end

    assert_equal 4, retry_queue(1).message_count
    assert_equal 0, retry_queue(2).message_count
    assert_equal 0, error_queue.message_count

    wait_for_retry 1

    assert_equal 0, retry_queue(1).message_count
    assert_equal 4, retry_queue(2).message_count
    assert_equal 0, error_queue.message_count

    wait_for_retry 2

    assert_equal 0, retry_queue(2).message_count
    assert_equal 4, error_queue.message_count
  end

  private

  def retry_delay(retry_number)
    FailingWorker::DELAY_VALUES[retry_number - 1]
  end

  def retry_queue(retry_number)
    delay = retry_delay(retry_number)
    channel.queue("sneaker_handlers.configurable_back_test.retry.#{delay}",
      durable: false,
      arguments: {
        :"x-dead-letter-exchange" => "sneakers_handlers",
        :"x-dead-letter-routing-key" => "sneaker_handlers.configurable_back_test",
        :"x-message-ttl" => delay * 1_000,
        :"x-expires" => delay * 1_000 * 2,
      }
    )
  end

  def wait_for_retry(retry_number)
    sleep retry_delay(retry_number)
  end

  def error_queue
    channel.queue("sneaker_handlers.configurable_back_test.error")
  end

  def channel
    @channel ||= begin
                   connection = Bunny.new.start
                   connection.create_channel
                 end
  end

  def cleanup!
    channel.exchange_delete("sneakers_handlers")
    channel.exchange_delete("sneakers_handlers.retry")
    channel.exchange_delete("sneakers_handlers.error")

    [FailingWorker].each do |worker|
      channel.queue_delete(worker.queue_name)
      channel.queue_delete(worker.queue_name + ".error")
      FailingWorker::DELAY_VALUES.each do |x|
        channel.queue_delete(worker.queue_name + ".retry.#{x}")
      end
    end
  end
end
