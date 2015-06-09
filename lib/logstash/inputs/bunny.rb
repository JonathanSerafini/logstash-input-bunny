# encoding: utf-8
require "logstash/inputs/threadable"
require "logstash/namespace"
require "logstash/inputs/rabbitmq/bunny"

# Pull events from a RabbitMQ exchange.
#
# The default settings will create an entirely transient queue and listen for all messages by default.
# If you need durability or any other advanced settings, please set the appropriate options
#
# This has been tested with Bunny 1.7.x, which supports RabbitMQ 2.x and 3.x. You can
# find links to both here:
#
# * Bunny - <https://github.com/ruby-amqp/bunny>
class LogStash::Inputs::Bunny < LogStash::Inputs::Threadable
  config_name "bunny"

  #
  # Connection
  #

  config :host,         validate: :string, required: true
  config :port,         validate: :number, default: 5672
  config :user,         validate: :string, default: "guest"
  config :password,     validate: :password, default: "guest"
  config :vhost,        validate: :string, default: "/"

  config :ssl,          validate: :boolean, :default => false
  config :ssl_cert,     validate: :string
  config :ssl_key,      validate: :string
  config :ssl_ca_cert,  validate: :string
  config :verify_peer,  validate: :boolean, default: false

  #
  # Queue & Consumer
  #

  #
  # The Bunny plugin's approach differs from Logstash's RabbitMQ plugin in that
  # we require an exchange so that we can create & bind multiple queues based
  # on routing keys.
  #
  # In addition, rather than accepting a single string for the queue, we now
  # accept a hash of queue > routing_key pairs. 
  #

  config :exchange,     validate: :string, required: true
  config :queues,       validate: :hash, required: true 

  config :prefetch,     validate: :number, default: 20
  config :ack,          validate: :boolean, default: true

  #
  # Queue Durability / Persistence
  #

  # Create durable queues so that they survive broker restarts
  config :durable,      validate: :boolean, default: false

  # Delete queues once the last consumer disconnects.
  config :auto_delete,  validate: :boolean, default: false

  # Is the queue exclusive? Exclusive queues can only be used by the connection
  # that declared them and will be deleted when it is closed (e.g. due to a Logstash
  # restart).
  config :exclusive,    validate: :boolean, default: false

  # Passive queue creation? Useful for checking queue existance without modifying server state
  config :passive,      validate: :boolean, default: false

  #
  # Initialization
  #

  def initialize(params)
    params["codec"] = "json" if !params["codec"]
    super
  end

  def input_tag
    "#{@vhost}/#{@exchange}"
  end

  def message(severity, message, data={})
    message = "Bunny[#{input_tag}] #{message}"
    @logger.send(severity, message, data)
  end

  def register
    require "bunny"

    message(:info, "registering input", host: @host)

    @vhost       ||= Bunny::DEFAULT_HOST
    @port        ||= AMQ::Protocol::DEFAULT_PORT

    @settings = {
      vhost: @vhost,
      host:  @host,
      port:  @port,
      user:  @user,
      pass:  @password.value,
      tls:   @ssl,
      automatically_recover: false
    }

    if @ssl 
      @settings[:verify_peer] = @verify_peer
    end

    if @ssl and @ssl_cert
      @settings[:tls_cert]            = @ssl_cert
      @settings[:tls_key]             = @ssl_key
      @settings[:tls_ca_certificates] = [@ssl_ca_cert] if @ssl_ca_cert
    end
  end
  
  def run(output_queue)
    @output_queue = output_queue
    delay = Bunny::Session::DEFAULT_NETWORK_RECOVERY_INTERVAL * 2

    begin
      setup
      consume
    rescue Bunny::NetworkFailure, Bunny::ConnectionClosedError,
           Bunny::ConnectionLevelException, Bunny::TCPConnectionFailed,
           OpenSSL::SSL::SSLError => e

      @consumers.each do |name, consumer|
        error = "connection error: #{e.message}. Retrying in #{delay} seconds."
        message(:error, error)
        consumer.channel.maybe_kill_consumer_work_pool!
      end

      sleep delay
      retry

    rescue LogStash::ShutdownSignal
      release

    rescue Exception => e
      message(:error, "unhandled exception: #{e.inspect}", backtrace: e.backtrace)

    end
  end

  def teardown
    release
    finished
  end

  def setup
    return if terminating?

    #
    # With multiple threads, Logstash/Jruby/Bunny combo seems to fail ssl
    # handshake on the first 1-2 connection attempts when restarting Logstash.
    #
    # optionally, look at : settings[:connect_timeout]
    #
    begin
      connection_attempt ||= 1
      message(:debug, "connecting")
      @connection = Bunny.new(@settings)
      @connection.start
    rescue OpenSSL::SSL::SSLError
      message(:warn, "ssl handshake failed, retrying", 
              attempt: connection_attempt)
      connection_attempt += 1
      sleep(5)
      retry unless connection_attempt > 3
    end

    @consumers = {}
    @queues.each do |queue_name, routing_key|
      consumer = "#{queue_name}#{routing_key}"
      message(:debug, "creating consumer #{consumer}")

      channel = @connection.create_channel.tap do |channel|
        channel.prefetch(@prefetch)
      end
      
      queue = channel.queue(@queue,
                            durable: @durable,
                            auto_delete: @auto_delete,
                            exclusive: @exclusive,
                            passive: @passive)

      queue.bind(@exchange, routing_key: routing_key)

      @consumers[consumer] = Bunny::Consumer.
                              new(channel, queue, nil, !@ack, @exclusive)
    end
  end

  def release
    @consumers.each do |name, consumer|
      consumer.cancel
      consumer.channel.close if consumer.channel and consumer.channel.open?
    end
    @consumers = []

    @connection.close if @connection and @connection.open?
  end

  def consume
    @consumers.each do |name, consumer|
      message(:info, "consuming events from #{name}")

      consumer.on_delivery do |delivery_info, properties, data|
        @codec.decode(data) do |event|
          decorate(event)
          event["bunny"] ||= {}
          event["bunny"]["key"] = delivery_info.routing_key
          event["bunny"]["queue"] = consumer.queue_name
          @output_queue << event
        end

        consumer.channel.acknowledge(delivery_info.delivery_tag) if @ack
      end

      consumer.queue.subscribe_with(consumer, block: false)
    end

    # Join all consummer threads to the current thread and wait to complete
    @consummers.each do |name, consumer|
      consumer.channel.work_pool.join
    end
  end
end # class LogStash::Inputs::RabbitMQ

