# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/json"

# This input plugin permits to retrieve metrics from remote Java applications using JMX.
# Every `polling_frequency`, it scans a folder containing json configuration 
# files describing JVMs to monitor with metrics to retrieve.
# Then a pool of threads will retrieve metrics and create events.
#
# ## The configuration:
#
# In Logstash configuration, you must set the polling frequency,
# the number of thread used to poll metrics and a directory absolute path containing
# json files with the configuration per jvm of metrics to retrieve.
# Logstash input configuration example:
# [source,ruby]
#     jmx {
#       //Required
#       path => "/apps/logstash_conf/jmxconf"
#       //Optional, default 60s
#       polling_frequency => 15
#       type => "jmx"
#       //Optional, default 4
#       nb_thread => 4
#     }
#
# Json JMX configuration example:
# [source,js]
#     {
#       //Required, JMX listening host/ip
#       "host" : "192.168.1.2",
#       //Required, JMX listening port
#       "port" : 1335,
#       //Optional, the username to connect to JMX
#       "username" : "user",
#       //Optional, the password to connect to JMX
#       "password": "pass",
#       //Optional, use this alias as a prefix in the metric name. If not set use <host>_<port>
#       "alias" : "test.homeserver.elasticsearch",
#       //Required, list of JMX metrics to retrieve
#       "queries" : [
#       {
#         //Required, the object name of Mbean to request
#         "object_name" : "java.lang:type=Memory",
#         //Optional, use this alias in the metrics value instead of the object_name
#         "object_alias" : "Memory"
#       }, {
#         "object_name" : "java.lang:type=Runtime",
#         //Optional, set of attributes to retrieve. If not set retrieve
#         //all metrics available on the configured object_name.
#         "attributes" : [ "Uptime", "StartTime" ],
#         "object_alias" : "Runtime"
#       }, {
#         //object_name can be configured with * to retrieve all matching Mbeans
#         "object_name" : "java.lang:type=GarbageCollector,name=*",
#         "attributes" : [ "CollectionCount", "CollectionTime" ],
#         //object_alias can be based on specific value from the object_name thanks to ${<varname>}.
#         //In this case ${type} will be replaced by GarbageCollector...
#         "object_alias" : "${type}.${name}"
#       }, {
#         "object_name" : "java.nio:type=BufferPool,name=*",
#         "object_alias" : "${type}.${name}"
#       } ]
#     }
#
# Here are examples of generated events. When returned metrics value type is 
# number/boolean it is stored in `metric_value_number` event field
# otherwise it is stored in `metric_value_string` event field.
# [source,ruby]
#     {
#       "@version" => "1",
#       "@timestamp" => "2014-02-18T20:57:27.688Z",
#       "host" => "192.168.1.2",
#       "path" => "/apps/logstash_conf/jmxconf",
#       "type" => "jmx",
#       "metric_path" => "test.homeserver.elasticsearch.GarbageCollector.ParNew.CollectionCount",
#       "metric_value_number" => 2212
#     }
#
# [source,ruby]
#     {
#       "@version" => "1",
#       "@timestamp" => "2014-02-18T20:58:06.376Z",
#       "host" => "localhost",
#       "path" => "/apps/logstash_conf/jmxconf",
#       "type" => "jmx",
#       "metric_path" => "test.homeserver.elasticsearch.BufferPool.mapped.ObjectName",
#       "metric_value_string" => "java.nio:type=BufferPool,name=mapped"
#     }
#
class LogStash::Inputs::HikariCP < LogStash::Inputs::Base
  config_name 'hikaricp'
  milestone 1

  #Class Var
  attr_accessor :regexp_group_alias_object
  attr_accessor :queue_conf

  # Host
  config :host, :validate => :string, :required => true

  # Port
  config :port, :validate => :string, :required => true

  # Pool
  config :pool, :validate => :string, :required => true

  # Pool
  config :username, :validate => :string

  # Pool
  config :password, :validate => :string

  # Indicate interval between two jmx metrics retrieval
  # (in s)
  config :polling_frequency, :validate => :number, :default => 60

  # Indicate number of thread launched to retrieve metrics
  config :nb_thread, :validate => :number, :default => 4

  #Error messages
  MISSING_CONFIG_PARAMETER = "Missing parameter '%s'."
  BAD_TYPE_CONFIG_PARAMETER = "Bad type for parameter '%{param}', expecting '%{expected}', found '%{actual}'."
  MISSING_QUERY_PARAMETER = "Missing parameter '%s' in queries[%d]."
  BAD_TYPE_QUERY = "Bad type for queries[%{index}], expecting '%{expected}', found '%{actual}'."
  BAD_TYPE_QUERY_PARAMETER = "Bad type for parameter '%{param}' in queries[%{index}], expecting '%{expected}', found '%{actual}'."

  private
  def send_event_to_queue(queue, host, active, idle, total, maxPoolSize, name)
    @logger.debug('Send event to queue to be processed by filters/outputs')
    event = LogStash::Event.new
    event.set('host', host)
    event.set('path', @path)
    event.set('type', @type)
    event.set('active', active)
    event.set('idle', idle)
    event.set('total', total)
    event.set('maxPoolSize', max)
    event.set('poolName', name)

    decorate(event)
    queue << event
  end

  # Thread function to retrieve metrics from JMX
  private
  def thread_jmx(queue_conf, queue)
    while true
      begin
        @logger.debug('Wait config to retrieve from queue conf')
        thread_hash_conf = queue_conf.pop
        @logger.debug("Retrieve config #{thread_hash_conf} from queue conf")

        @logger.debug('Check if jmx connection need a user/password')
        if @login.nil? and @password.nil?
          @logger.debug("Connect to #{host]}:#{port]} with user #{username}")
          jmx_connection = JMX::MBean.connection :host => @host,
                                                 :port => @port,
                                                :username => @username,
                                               :password => @password
        else
          @logger.debug("Connect to #{thread_hash_conf['host']}:#{thread_hash_conf['port']}")
          jmx_connection = JMX::MBean.connection :host => @host,
                                                 :port => @port
        end


          poolConfigBeans = JMX::MBean.find_all_by_name("com.zaxxer.hikari:type=PoolConfig (*)", :connection => jmx_connection)

          if poolConfigBeans.length > 0
            poolConfigBeans.each do |poolConfigBean|
=begin
              jmx_object_name.attributes.each_key do |attribute|
                @logger.info(attribute)
              end
=end
              maxPoolSize = poolConfigBean.MaximumPoolSize
              name = poolConfigBean.PoolName

              poolMBean = JMX::MBean.find_by_name("com.zaxxer.hikari:type=Pool (#{name})", :connection => jmx_connection)

              active = poolMBean.send("active_connections")
              idle = poolMBean.send("idle_connections")
              total = poolMBean.send("total_connections")
              send_event_to_queue(queue, "#{host}:#{port}", active, idle, total, maxPoolSize, name)

            end
          else
            @logger.warn("No jmx object found for #{query['object_name']}")
          end

          jmx_connection.close
          rescue LogStash::ShutdownSignal
          break #free
          rescue Exception => ex
          @logger.error(ex.message)
          @logger.error(ex.backtrace.join("\n"))
        end
      end
    end

    public
    def register
      require 'thread'
      require 'jmx4r'

      @logger.info("Create queue dispatching JMX requests to threads")
      @queue_conf = Queue.new

      @logger.info("Compile regexp for group alias object replacement")
      @regexp_group_alias_object = Regexp.new('(?:\${(.*?)})+')
    end

    public
    def run(queue)
      begin
        threads = []
        @logger.info("Initialize #{@nb_thread} threads for JMX metrics collection")
        @nb_thread.times do
          threads << Thread.new {thread_jmx(@queue_conf, queue)}
        end

        while !@interrupted
          @queue_conf << 1
          @logger.debug('Wait until the queue conf is empty')
          delta=0
          until @queue_conf.empty?
            @logger.debug("There are still #{@queue_conf.size} messages in the queue conf. Sleep 1s.")
            delta=delta+1
            sleep(1)
          end
          wait_time=@polling_frequency-delta
          if wait_time>0
            @logger.debug("Wait #{wait_time}s (#{@polling_frequency}-#{delta}(seconds wait until queue conf empty)) before to launch again a new jmx metrics collection")
            sleep(wait_time)
          else
            @logger.warn("The time taken to retrieve metrics is more important than the retrieve_interval time set.
                       \nYou must adapt nb_thread, retrieve_interval to the number of jvm/metrics you want to retrieve.")
          end
        end
      rescue LogStash::ShutdownSignal
        #exiting
      rescue Exception => ex
        @logger.error(ex.message)
        @logger.error(ex.backtrace.join("\n"))
      ensure
        threads.each do |thread|
          thread.raise(LogStash::ShutdownSignal) if thread.alive?
        end
      end

    end

    public
    def close
      @interrupted = true
    end # def close
  end
