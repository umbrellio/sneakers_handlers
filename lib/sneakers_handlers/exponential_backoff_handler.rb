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
  class ExponentialBackoffHandler
    attr_reader :queue, :channel, :options, :max_retries, :backoff_function

    DEFAULT_MAX_RETRY_ATTEMPTS = 25
    DEFAULT_BACKOFF_FUNCTION = -> (attempt_number) { (attempt_number + 1) ** 2 }

    def initialize(channel, queue, options)
      @queue = queue
      @channel = channel
      @options = options
      @max_retries = options[:max_retries] || DEFAULT_MAX_RETRY_ATTEMPTS
      @backoff_function  = options[:backoff_function] || DEFAULT_BACKOFF_FUNCTION
      @retry_queues_mutex = Mutex.new
      @primary_exchange = create_primary_exchange
      @error_exchange = create_error_exchange!

      queue.bind(primary_exchange, routing_key: queue.name)
    end

    def acknowledge(delivery_info, _, _)
      channel.acknowledge(delivery_info.delivery_tag, false)
    end

    def reject(delivery_info, properties, message, _requeue = true)
      retry_message(delivery_info, properties, message, :reject)
    end

    def error(delivery_info, properties, message, err)
      retry_message(delivery_info, properties, message, err.inspect)
    end

    def timeout(delivery_info, properties, message)
      retry_message(delivery_info, properties, message, :timeout)
    end

    def noop(_delivery_info, _properties, _message)
    end

    private

    attr_reader :retry_queues_mutex, :primary_exchange, :error_exchange

    def retry_message(delivery_info, properties, message, reason)
      attempt_number = death_count(properties[:headers])
      headers = (properties[:headers] || {}).merge(rejection_reason: reason.to_s)
      headers = remove_delayed_message_header(headers)

      if attempt_number < max_retries
        delay = backoff_function.call(attempt_number)

        log(message: "msg=retrying, delay=#{delay}, count=#{attempt_number}, properties=#{properties}, reason=#{reason}")

        routing_key = "#{queue.name}.#{delay}"

        retry_queues_mutex.synchronize do
          retry_queue = create_retry_queue!(delay)
          retry_queue.bind(primary_exchange, routing_key: routing_key)
        end

        primary_exchange.publish(message, routing_key: routing_key, headers: headers)
      else
        log(message: "msg=erroring, count=#{attempt_number}, properties=#{properties}")
        error_exchange.publish(message, routing_key: dlx_routing_key, headers: headers)
      end

      acknowledge(delivery_info, properties, message)
    rescue Bunny::ConnectionClosedError => e
      log(level: :error, message: "msg=connection_closed_error, error='#{e.message}'")
      channel.close if channel.open?
      raise e
    rescue => e
      log(level: :error, message: "msg=unexpected_handler_error, error='#{e.message}'")

      # In the case of an unhandled exception, we need to `nack` the message so
      # it doesn't get stuck in the `unacked` state until this process dies.
      channel.nack(delivery_info.delivery_tag, multiple = false, requeue = true) if channel.open?

      raise e
    end

    # This is the header used by the `rabbitmq-delayed-message-exchange`
    # plugin.  We need to remove it otherwise the messages that are published
    # to the retry queues would also be delayed. This becomes a bigger problem
    # when we have queues that expire (using `x-expires`) shortly after they
    # are created. If, for instance, we have a retry queue that expires in 5
    # seconds, and we publish a message with `x-delay` of `6000`, by the time
    # this message is ready to be published, the queue doesn't exist anymore,
    # resulting in a message loss.
    def remove_delayed_message_header(headers)
      headers.reject { |k| k == "x-delay" }
    end

    def death_count(headers)
      return 0 if headers.nil? || headers["x-death"].nil?

      headers["x-death"].inject(0) do |sum, x_death|
        sum + x_death["count"] if x_death["queue"] =~ /^#{queue.name}/
      end
    end

    def log(message:, level: :info)
      Sneakers.logger.send(level) do
        "[#{self.class}] #{message}"
      end
    end

    def create_exchange(name, type = "topic")
      log(message: "creating exchange=#{name}")

      channel.exchange(name, type: type, durable: options[:exchange_options][:durable])
    end

    def create_primary_exchange
      create_exchange(options[:exchange], options[:exchange_options][:type])
    end

    def create_error_exchange!
      create_exchange(dlx_exchange_name).tap do |exchange|
        queue = create_queue!("#{@queue.name}.error")
        queue.bind(exchange, routing_key: dlx_routing_key)
      end
    end

    def dlx_routing_key
      options[:queue_options][:arguments].fetch("x-dead-letter-routing-key")
    end

    def dlx_exchange_name
      options[:queue_options][:arguments].fetch("x-dead-letter-exchange")
    end

    def create_retry_queue!(delay)
      queue_name = "#{queue.name}.retry.#{delay}"

      # When we create a new queue, `Bunny` stores its name in an internal cache.
      # The problem is that as we are creating ephemeral queues that can expire shortly
      # after they are created, this cached queue may not exist anymore when we try to
      # publish a second message to it.
      # Removing queues from the cache guarantees that `Bunny` will always try
      # to check if they exist, and when they don't, it will create them for us.
      channel.deregister_queue_named(queue_name)

      create_queue!(
        queue_name,
        :"x-dead-letter-exchange" => options[:exchange],
        :"x-dead-letter-routing-key" => queue.name,
        :"x-message-ttl" => delay * 1_000,
        :"x-expires" => delay * 1_000 * 2
      )
    end

    def create_queue!(name, **arguments)
      durable = options[:queue_options][:durable]
      arguments = { :"x-queue-type" => "quorum", **arguments } if durable
      channel.queue(name, durable: durable, arguments: arguments)
    rescue Bunny::PreconditionFailed
      channel.queue_delete(name)
      retry
    end
  end
end
