require_relative "../../lib/sneakers_handlers/configurable_backoff_handler"

module ConfigurableBackoffHandlerTestBase
  def self.included(base)
    base.extend ClassMethods
  end

  module ClassMethods
    def queue_name(value = (get = true))
      @queue_name = value unless get
      @queue_name ||= 'sneakers_handlers.configurable_backoff_handler'
    end

    def worker_class
      @worker_class ||= @worker_class_factory[self]
    end

    def expected_retries(array = (get = true))
      @expected_retries = array unless get
      @expected_retries ||= []
    end

    protected

    def handler(handler_class, handler_opts = {})
      @worker_class_factory = proc { |me|
        Class.new do
          include Sneakers::Worker

          from_queue me.queue_name, {
            ack: true,
            durable: false,
            exchange: "#{me.queue_name}.exchange",
            exchange_type: :topic,
            routing_key: ['lifecycle.created'],
            handler: handler_class,
            arguments: {
              "x-dead-letter-exchange" => "#{me.queue_name}.error.exchange",
              "x-dead-letter-routing-key" => me.queue_name
            }
          }.merge(handler_opts)

          def work(payload)
            return payload.to_sym
          end
        end
      }
    end
  end

  def queue_name
    self.class.queue_name
  end

  def run_worker!
    self.class.worker_class.new.run
  end

  def expected_retries
    self.class.expected_retries
  end

  def setup
    cleanup!
  end

  def teardown
    cleanup!
  end

  def run_retry_end_to_end(entry_exchange, routing_key)
    ['timeout', 'error', 'requeue', 'reject'].each do |type_of_failure|
      run_worker!
      entry_exchange.publish(type_of_failure, routing_key: routing_key)
      sleep 0.1
    end

    expected_retries.each do |current_delay|
      assert_equal 4, retry_queue(current_delay).message_count

      other_delays = expected_retries - [current_delay]
      other_delays.each do |other_delay|
        assert_equal 0, retry_queue(other_delay).message_count
      end

      assert_equal 0, error_queue.message_count

      sleep current_delay
    end

    assert_equal 4, error_queue.message_count

    expected_retries.each do |delay|
      assert_equal 0, retry_queue(delay).message_count
    end
  end

  def test_handler_retries_with_ttl_retry_queues
    run_retry_end_to_end exchange, 'lifecycle.created'
  end

  def test_works_when_shoveling_messages
    run_retry_end_to_end channel.default_exchange, queue_name
  end

  protected

  def channel
    @channel ||= begin
                   connection = Bunny.new.start
                   connection.create_channel
                 end
  end

  def exchange
    @exchange ||= channel.topic("#{queue_name}.exchange", durable: false)
  end

  def retry_queue(delay)
    channel.queues.clear
    delay_ms = s_to_ms(delay)
    channel.queue("#{queue_name}.retry.#{delay_ms}",
      durable: false,
      arguments: {
        :"x-dead-letter-exchange" => "#{queue_name}.exchange",
        :"x-dead-letter-routing-key" => queue_name,
        :"x-message-ttl" => delay_ms,
        :"x-expires" => delay_ms * 2,
      }
    )
  end

  def error_queue
    @error_queue ||= channel.queue("#{queue_name}.error")
  end

  def cleanup!
    channel.exchange_delete("#{queue_name}.exchange")
    channel.exchange_delete("#{queue_name}.error.exchange")

    channel.queue_delete(queue_name)
    channel.queue_delete("#{queue_name}.error")

    expected_retries.each do |delay|
      channel.queue_delete("#{queue_name}.retry.#{s_to_ms(delay)}")
    end
  end

  def s_to_ms(delay)
    (delay * 1_000).to_i
  end
end
