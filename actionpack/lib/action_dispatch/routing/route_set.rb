# frozen_string_literal: true

require "active_support/core_ext/array/extract_options"

module ActionDispatch
  module Routing
	  class RouteSet
		@@METHODS = ["GET","POST","DELETE","PUT","PATCH"]

public 
		def self.new_with_config (config)
		  RouteSet.new
		end

		def initialize
		  @tree = Rr3::Tree.new(10)
		  @routes = []
		end

		def not_found
		  ::Rack::Response.new("No route found", 404).finish
		end

		def rtrim (path)
		  path.chomp('/') if path.length > 1
		end
		
		def call(env)
		  http_method =  @@METHODS.find_index(env["REQUEST_METHOD"])
		  return not_found unless http_method
		  path = rtrim(env["REQUEST_PATH"])
		  result = @tree.match (1 << http_method), path
		  return not_found unless result
		  route = @routes[result["data"]]
		  route.call(env, result["slugs"])
		end

		def build_route (slugs, to)
		  lambda { |env, members|
			params = Hash[slugs.zip(members)]
			to.call(env, params)
		  }
		end

		def add (methods, path, to)
		  http_method = 0
		  methods.each do |m| http_method = http_method | (1 << @@METHODS.find_index(m)) if m end
		  
		  slugs = []
		  counter = 0
		  path = path.gsub(/\{([^\\^:}]+)(:[^\}]+)?\}/) do |m|
			slugs.push($1)
			counter = counter + 1
			extra = $2
			"{a"+counter.to_s+(extra ? extra : "")+"}"
		  end

		  @tree.insert(http_method, path, @routes.length)
		  @routes.push(build_route(slugs, to))
		end

		def add_controller(methods, path, klass, name)
		  to = lambda { |env, members|
			req = ActionDispatch::Request.new(env)
			req.path_parameters = members
			res = klass.make_response! req

			resolved_name = (defined? name.call) ? name.call(env["REQUEST_METHOD"]) : name
			klass.dispatch(resolved_name, req, res)
		  }

		  add(methods, path, to)
		end

		def should_resource(name, except, only)
		  return ((except == nil || !except.include?(name)) && (only == nil || only.include?(name)))
		end

		def _create_or_index(method)
		  return "create" if method == "POST"
		  return "index"
		end
		def _single(method)
		  return "show" if method == "GET"
		  return "destroy" if method == "DELETE"
		  return "update" # PUT or PATCH
		end

		def add_resource(path, controller, param = "id:[0-9]+", except: nil, only: nil)
		  path = rtrim(path)
		  should_index = should_resource("index", except, only)
		  should_create = should_resource("create", except, only)
		  if should_index || should_create then
			add_controller([should_index ? "GET" : nil, should_create ? "POST" : nil], path, controller, method(:_create_or_index))
		  end

		  should_show = should_resource("show", except, only)
		  should_destroy = should_resource("destroy", except, only)
		  should_update = should_resource("update", except, only)
		  if should_show || should_destroy || should_update then
			add_controller([should_show ? "GET" : nil, should_destroy ? "DELETE" : nil, should_update ? "PUT" : nil, should_update ? "PATCH" : nil], path + "/{"+param.to_s+"}", controller, method(:_single))
		  end
		end

		module MountedHelpers
		end
		module UrlHelpers
		end

		def prepend (&block)

		end

		def append (&block)

		end
		
		# Generates a url from Rack env and identifiers or significant keys.
		#
		# To generate a url by named route, pass the name in as a `Symbol`.
		#   url(env, :dashboard) # => "/dashboard"
		#
		# Additional parameters can be passed in as a hash
		#   url(env, :people, :id => "1") # => "/people/1"
		#
		# If no name route is given, it will fall back to a slower
		# generation search.
		#   url(env, :controller => "people", :action => "show", :id => "1")
		#     # => "/people/1"
		def url_helpers(supports_path = true)
		  UrlHelpers
		end

		def default_url_options

		end

		def mounted_helpers
		  MountedHelpers
		end

		def define_mounted_helper (name)

		end

		def disable_clear_and_finalize= (value)

		end

		def disable_clear_and_finalize
		  return True
		end

		def clear!

		end

		def eager_load!

		end

		def env_key
		  {}
		end

		def finalize!
		  @tree.compile!
		end

		def dump
		  @tree.dump 10
		end
	  end
	end
end
