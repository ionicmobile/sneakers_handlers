module SneakersHandlers
  class ExponentialBackoffHandler
    attr_reader :queue, :channel, :options, :max_retries

    DEFAULT_MAX_RETRY_ATTEMPTS = 25

    def initialize(channel, queue, options)
      @queue = queue
      @channel = channel
      @options = options
      @max_retries = options[:max_retries] || DEFAULT_MAX_RETRY_ATTEMPTS

      create_error_exchange!

      Array(@options[:routing_key]).each do |key|
        queue.bind(primary_exchange, routing_key: queue.name + "." + key + ".*")
      end
    end

    def acknowledge(delivery_info, _, _)
      channel.acknowledge(delivery_info.delivery_tag, false)
    end

    def reject(delivery_info, properties, message, _requeue = true)
      retry_message(delivery_info, properties, message, :reject)
    end

    def error(delivery_info, properties, message, err)
      retry_message(delivery_info, properties, message, err)
    end

    def timeout(delivery_info, properties, message)
      retry_message(delivery_info, properties, message, :timeout)
    end

    def noop(_delivery_info, _properties, _message)
    end

    private

    def retry_message(delivery_info, properties, message, reason)
      attempt_number = death_count(properties[:headers])

      routing_key_segments = (queue.name + "." + delivery_info[:routing_key].gsub(queue.name + ".", "")).split(".")
      routing_key_segments.pop if Integer(routing_key_segments.last) rescue nil

      if attempt_number < max_retries
        delay = seconds_to_delay(attempt_number)

        log("msg=retrying, delay=#{delay}, count=#{attempt_number}, properties=#{properties}, reason=#{reason}")

        routing_key_segments << delay
        routing_key = routing_key_segments.join(".")

        retry_queue = create_retry_queue!(delay)
        retry_queue.bind(retry_exchange, routing_key: routing_key)

        retry_exchange.publish(message, routing_key: routing_key, headers: properties[:headers])
      else
        log("msg=erroring, count=#{attempt_number}, properties=#{properties}")

        channel.reject(delivery_info.delivery_tag)
      end

      acknowledge(delivery_info, properties, message)
    end

    def death_count(headers)
      return 0 if headers.nil? || headers["x-death"].nil?

      headers["x-death"].inject(0) do |sum, x_death|
        sum + x_death["count"] if x_death["queue"] =~ /^#{queue.name}/
      end
    end

    def log(message)
      Sneakers.logger.debug do
        "[#{self.class}] [queue=#{@primary_queue_name}] #{message}"
      end
    end

    def durable_exchanges?
      options[:exchange_options][:durable]
    end

    def durable_queues?
      options[:queue_options][:durable]
    end

    def create_exchange(name)
      log("creating exchange=#{name}")

      @channel.exchange(name, type: "topic", durable: durable_exchanges?)
    end

    def retry_exchange
      @retry_exchange ||= create_exchange("#{options[:exchange]}.retry")
    end

    def primary_exchange
      @primary_exchange ||= create_exchange("#{options[:exchange]}")
    end

    def create_error_exchange!
      @error_exchange ||= create_exchange("#{options[:exchange]}.error").tap do |exchange|
        queue = @channel.queue("#{@queue.name}.error", durable: durable_queues?)

        Array(@options[:routing_key]).each do |key|
          queue.bind(exchange, routing_key: @queue.name + "." + key)
          queue.bind(exchange, routing_key: @queue.name + "." + key + ".*")
        end
      end
    end

    def create_retry_queue!(delay)
      @channel.queue("#{queue.name}.retry.#{delay}",
       durable: durable_queues?,
       arguments: {
         :"x-dead-letter-exchange" => options[:exchange],
         :"x-message-ttl" => delay * 1_000,
         :"x-expires" => delay * 1_000 * 2
       }
      )
    end

    def seconds_to_delay(count)
      (count + 1) ** 2
    end
  end
end
