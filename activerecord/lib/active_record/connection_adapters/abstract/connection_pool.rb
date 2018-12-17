# frozen_string_literal: true

require "thread"
require "concurrent/map"
require "monitor"

module ActiveRecord
  # Raised when a connection could not be obtained within the connection
  # acquisition timeout period: because max connections in pool
  # are in use.
  class ConnectionTimeoutError < ConnectionNotEstablished
  end

  # Raised when a pool was unable to get ahold of all its connections
  # to perform a "group" action such as
  # {ActiveRecord::Base.connection_pool.disconnect!}[rdoc-ref:ConnectionAdapters::ConnectionPool#disconnect!]
  # or {ActiveRecord::Base.clear_reloadable_connections!}[rdoc-ref:ConnectionAdapters::ConnectionHandler#clear_reloadable_connections!].
  class ExclusiveConnectionTimeoutError < ConnectionTimeoutError
  end

  module ConnectionAdapters
    # Connection pool base class for managing Active Record database
    # connections.
    #
    # == Introduction
    #
    # A connection pool synchronizes thread access to a limited number of
    # database connections. The basic idea is that each thread checks out a
    # database connection from the pool, uses that connection, and checks the
    # connection back in. ConnectionPool is completely thread-safe, and will
    # ensure that a connection cannot be used by two threads at the same time,
    # as long as ConnectionPool's contract is correctly followed. It will also
    # handle cases in which there are more threads than connections: if all
    # connections have been checked out, and a thread tries to checkout a
    # connection anyway, then ConnectionPool will wait until some other thread
    # has checked in a connection.
    #
    # == Obtaining (checking out) a connection
    #
    # Connections can be obtained and used from a connection pool in several
    # ways:
    #
    # 1. Simply use {ActiveRecord::Base.connection}[rdoc-ref:ConnectionHandling.connection]
    #    as with Active Record 2.1 and
    #    earlier (pre-connection-pooling). Eventually, when you're done with
    #    the connection(s) and wish it to be returned to the pool, you call
    #    {ActiveRecord::Base.clear_active_connections!}[rdoc-ref:ConnectionAdapters::ConnectionHandler#clear_active_connections!].
    #    This will be the default behavior for Active Record when used in conjunction with
    #    Action Pack's request handling cycle.
    # 2. Manually check out a connection from the pool with
    #    {ActiveRecord::Base.connection_pool.checkout}[rdoc-ref:#checkout]. You are responsible for
    #    returning this connection to the pool when finished by calling
    #    {ActiveRecord::Base.connection_pool.checkin(connection)}[rdoc-ref:#checkin].
    # 3. Use {ActiveRecord::Base.connection_pool.with_connection(&block)}[rdoc-ref:#with_connection], which
    #    obtains a connection, yields it as the sole argument to the block,
    #    and returns it to the pool after the block completes.
    #
    # Connections in the pool are actually AbstractAdapter objects (or objects
    # compatible with AbstractAdapter's interface).
    #
    # == Options
    #
    # There are several connection-pooling-related options that you can add to
    # your database connection configuration:
    #
    # * +pool+: maximum number of connections the pool may manage (default 5).
    # * +idle_timeout+: number of seconds that a connection will be kept
    #   unused in the pool before it is automatically disconnected (default
    #   300 seconds). Set this to zero to keep connections forever.
    # * +checkout_timeout+: number of seconds to wait for a connection to
    #   become available before giving up and raising a timeout error (default
    #   5 seconds).
    #
    #--
    # Synchronization policy:
    # * all public methods can be called outside +synchronize+
    # * access to these instance variables needs to be in +synchronize+:
    #   * @connections
    #   * @now_connecting
    # * private methods that require being called in a +synchronize+ blocks
    #   are now explicitly documented
	class ConnectionPool
	  class ImplicitConnectionForbiddenError < ::ActiveRecord::ConnectionTimeoutError ; end  
	
	  include MonitorMixin
	  include ActiveRecord::ConnectionAdapters::QueryCache::ConnectionPoolConfiguration

	  attr_accessor :automatic_reconnect, :checkout_timeout, :schema_cache
	  attr_reader :spec, :connections, :size, :reaper

	  class Reaper
		attr_reader :pool, :frequency
		def initialize(pool, frequency)
		  @pool = pool;
		  @frequency = frequency;
		end
		def stop; end
		def run; end
	  end
	  
	  def initialize(spec)
		trace "ConnectionPool.initialize", 5
		super()

		@spec = spec
		@checkout_timeout = (spec.config[:checkout_timeout] && spec.config[:checkout_timeout].to_f) || 5
		@reaper = Reaper.new(self, (spec.config[:reaping_frequency] && spec.config[:reaping_frequency].to_f))
		@reaper.run
		@size = (spec.config[:pool] && spec.config[:pool].to_i) || 5
		@thread_cached_conns = Concurrent::Map.new
		@connections = []
		@automatic_reconnect = true
		@now_connecting = 0
		@threads_blocking_new_connections = 0
		@lock_thread = false
	  end

	  def lock_thread=(lock_thread)
		trace "ConnectionPool.lock_thread"
		if lock_thread
		  @lock_thread = Thread.current
		else
		  @lock_thread = nil
		end
	  end

	  def forbid_implicit_checkout_for_thread!
		Thread.current[:active_record_forbid_implicit_connections] = true
	  end
	  
	  def real_connection
		@thread_cached_conns[connection_cache_key(@lock_thread || Thread.current)] ||= checkout
	  end

	  def connection
		trace "ConnectionPool.connection"
		cp = @thread_cached_conns[connection_cache_key(@lock_thread || Thread.current)]
		return cp if cp
		if Thread.current[:active_record_forbid_implicit_connections] then
		  raise ImplicitConnectionForbiddenError.new("Implicit ActiveRecord checkout attempted when Thread :force_explicit_connections set!")
		end
		@thread_cached_conns[connection_cache_key(@lock_thread || Thread.current)] ||= checkout
	  end

	  def active_connection?
		trace "ConnectionPool.active_connection?"
		@thread_cached_conns[connection_cache_key(Thread.current)]
	  end

	  def release_connection(owner_thread = Thread.current)
		trace "ConnectionPool.release_connection"
		if conn = @thread_cached_conns.delete(connection_cache_key(owner_thread))
		  checkin conn
		end
	  end

	  def with_connection
		trace "ConnectionPool.with_connection"
		unless conn = @thread_cached_conns[connection_cache_key(@lock_thread || Thread.current)]
		  conn = real_connection
		  fresh_connection = true
		end
		yield conn
	  ensure
		release_connection if fresh_connection
	  end

	  def connected?
		trace "ConnectionPool.connected?"
		synchronize { @connections.any? }
	  end

	  def disconnect(raise_on_acquisition_timeout = true)
		trace "ConnectionPool.disconnect"
		synchronize do
		  @connections.each do |conn|
			if conn.in_use?
			  conn.steal!
			  checkin conn
			end
			conn.disconnect!
		  end
		  @connections = []
		end
	  end

	  def disconnect!
		trace "ConnectionPool.disconnect!"
		disconnect(false)
	  end

	  def clear_reloadable_connections(raise_on_acquisition_timeout = true)
		trace "ConnectionPool.clear_reloadable_connections"
		synchronize do
		  @connections.each do |conn|
			if conn.in_use?
			  conn.steal!
			  checkin conn
			end
			conn.disconnect! if conn.requires_reloading?
		  end
		  @connections.delete_if(&:requires_reloading?)
		end
	  end

	  def clear_reloadable_connections!
		trace "ConnectionPool.clear_reloadable_connections!"
		clear_reloadable_connections(false)
	  end

	  def checkout(checkout_timeout = @checkout_timeout)
		trace "ConnectionPool.checkout"
		conn = checkout_new_connection
		conn.lease
		checkout_and_verify(conn)
	  end

	  def checkin(conn)
        conn.lock.synchronize do
	      conn.disconnect!
		  synchronize do
		    remove_connection_from_thread_cache conn

		    @connections.delete conn
		  end
		end
	  end

	  def remove(conn)
		trace "ConnectionPool.remove"
		synchronize do
		  remove_connection_from_thread_cache conn

		  @connections.delete conn
		end
	  end

	  def reap
		trace "ConnectionPool.reap"
	  end

	  def flush(minimum_idle = @idle_timeout)
		trace "ConnectionPool.flush"
	  end

	  def flush!
		trace "ConnectionPool.flush!"
	  end

	  def discard!
		trace "ConnectionPool.discard!"
		synchronize do
		  return if @connections.nil? # already discarded
		  @connections.each do |conn|
			conn.discard!
		  end
		  @connections = @thread_cached_conns = nil
		end
	  end

	  def num_waiting_in_queue
		trace "ConnectionPool.num_waiting_in_queue"
		0
	  end

	  def stat
		trace "ConnectionPool.stat"
		{}
	  end

	  private
		def connection_cache_key(thread)
		  thread
		end

		def remove_connection_from_thread_cache(conn, owner_thread = conn.owner)
		  @thread_cached_conns.delete_pair(connection_cache_key(owner_thread), conn)
		end
		alias_method :release, :remove_connection_from_thread_cache

		def checkout_new_connection
		  conn = new_connection
		  conn.pool = self
		  @connections << conn
		  conn
		end

		def new_connection
		  ActiveRecord::Base.send(spec.adapter_method, spec.config).tap do |conn|
			conn.schema_cache = schema_cache.dup if schema_cache
		  end
		end

		def checkout_and_verify(c)
		  c._run_checkout_callbacks do
			c.verify!
		  end
		  c
		rescue
		  remove c
		  c.disconnect!
		  raise
		end

		def trace(msg, depth = 1)
		  # puts msg
		  #caller[1..depth].each do |line|
		  #  puts "    \\---> #{line}"
		  # end
		end
	end

    # ConnectionHandler is a collection of ConnectionPool objects. It is used
    # for keeping separate connection pools that connect to different databases.
    #
    # For example, suppose that you have 5 models, with the following hierarchy:
    #
    #   class Author < ActiveRecord::Base
    #   end
    #
    #   class BankAccount < ActiveRecord::Base
    #   end
    #
    #   class Book < ActiveRecord::Base
    #     establish_connection :library_db
    #   end
    #
    #   class ScaryBook < Book
    #   end
    #
    #   class GoodBook < Book
    #   end
    #
    # And a database.yml that looked like this:
    #
    #   development:
    #     database: my_application
    #     host: localhost
    #
    #   library_db:
    #     database: library
    #     host: some.library.org
    #
    # Your primary database in the development environment is "my_application"
    # but the Book model connects to a separate database called "library_db"
    # (this can even be a database on a different machine).
    #
    # Book, ScaryBook and GoodBook will all use the same connection pool to
    # "library_db" while Author, BankAccount, and any other models you create
    # will use the default connection pool to "my_application".
    #
    # The various connection pools are managed by a single instance of
    # ConnectionHandler accessible via ActiveRecord::Base.connection_handler.
    # All Active Record models use this handler to determine the connection pool that they
    # should use.
    #
    # The ConnectionHandler class is not coupled with the Active models, as it has no knowledge
    # about the model. The model needs to pass a specification name to the handler,
    # in order to look up the correct connection pool.
    class ConnectionHandler
      def self.unowned_pool_finalizer(pid_map) # :nodoc:
        lambda do |_|
          discard_unowned_pools(pid_map)
        end
      end

      def self.discard_unowned_pools(pid_map) # :nodoc:
        pid_map.each do |pid, pools|
          pools.values.compact.each(&:discard!) unless pid == Process.pid
        end
      end

      def initialize
        # These caches are keyed by spec.name (ConnectionSpecification#name).
        @owner_to_pool = Concurrent::Map.new(initial_capacity: 2) do |h, k|
          # Discard the parent's connection pools immediately; we have no need
          # of them
          ConnectionHandler.discard_unowned_pools(h)

          h[k] = Concurrent::Map.new(initial_capacity: 2)
        end

        # Backup finalizer: if the forked child never needed a pool, the above
        # early discard has not occurred
        ObjectSpace.define_finalizer self, ConnectionHandler.unowned_pool_finalizer(@owner_to_pool)
      end

      def connection_pool_list
        owner_to_pool.values.compact
      end
      alias :connection_pools :connection_pool_list

      def establish_connection(config)
        resolver = ConnectionSpecification::Resolver.new(Base.configurations)
        spec = resolver.spec(config)

        remove_connection(spec.name)

        message_bus = ActiveSupport::Notifications.instrumenter
        payload = {
          connection_id: object_id
        }
        if spec
          payload[:spec_name] = spec.name
          payload[:config] = spec.config
        end

        message_bus.instrument("!connection.active_record", payload) do
          owner_to_pool[spec.name] = ConnectionAdapters::ConnectionPool.new(spec)
        end

        owner_to_pool[spec.name]
      end

      # Returns true if there are any active connections among the connection
      # pools that the ConnectionHandler is managing.
      def active_connections?
        connection_pool_list.any?(&:active_connection?)
      end

      # Returns any connections in use by the current thread back to the pool,
      # and also returns connections to the pool cached by threads that are no
      # longer alive.
      def clear_active_connections!
        connection_pool_list.each(&:release_connection)
      end

      # Clears the cache which maps classes.
      #
      # See ConnectionPool#clear_reloadable_connections! for details.
      def clear_reloadable_connections!
        connection_pool_list.each(&:clear_reloadable_connections!)
      end

      def clear_all_connections!
        connection_pool_list.each(&:disconnect!)
      end

      # Disconnects all currently idle connections.
      #
      # See ConnectionPool#flush! for details.
      def flush_idle_connections!
        connection_pool_list.each(&:flush!)
      end

      # Locate the connection of the nearest super class. This can be an
      # active or defined connection: if it is the latter, it will be
      # opened and set as the active connection for the class it was defined
      # for (not necessarily the current class).
      def retrieve_connection(spec_name) #:nodoc:
        pool = retrieve_connection_pool(spec_name)
        raise ConnectionNotEstablished, "No connection pool with '#{spec_name}' found." unless pool
        pool.connection
      end

      # Returns true if a connection that's accessible to this class has
      # already been opened.
      def connected?(spec_name)
        conn = retrieve_connection_pool(spec_name)
        conn && conn.connected?
      end

      # Remove the connection for this class. This will close the active
      # connection and the defined connection (if they exist). The result
      # can be used as an argument for #establish_connection, for easily
      # re-establishing the connection.
      def remove_connection(spec_name)
        if pool = owner_to_pool.delete(spec_name)
          pool.automatic_reconnect = false
          pool.disconnect!
          pool.spec.config
        end
      end

      # Retrieving the connection pool happens a lot, so we cache it in @owner_to_pool.
      # This makes retrieving the connection pool O(1) once the process is warm.
      # When a connection is established or removed, we invalidate the cache.
      def retrieve_connection_pool(spec_name)
        owner_to_pool.fetch(spec_name) do
          # Check if a connection was previously established in an ancestor process,
          # which may have been forked.
          if ancestor_pool = pool_from_any_process_for(spec_name)
            # A connection was established in an ancestor process that must have
            # subsequently forked. We can't reuse the connection, but we can copy
            # the specification and establish a new connection with it.
            establish_connection(ancestor_pool.spec.to_hash).tap do |pool|
              pool.schema_cache = ancestor_pool.schema_cache if ancestor_pool.schema_cache
            end
          else
            owner_to_pool[spec_name] = nil
          end
        end
      end

      private

        def owner_to_pool
          @owner_to_pool[Process.pid]
        end

        def pool_from_any_process_for(spec_name)
          owner_to_pool = @owner_to_pool.values.reverse.find { |v| v[spec_name] }
          owner_to_pool && owner_to_pool[spec_name]
        end
    end
  end
end
