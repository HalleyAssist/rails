# frozen_string_literal: true

module ActiveRecord
  module Coders # :nodoc:
    class JSON # :nodoc:
      def self.dump(obj)
        ActiveSupport::JSON.encode(obj)
      end

      def self.load(json)
        ret = ActiveSupport::JSON.decode(json) unless json.blank?
        obj.define_singleton_method(:to_json) do
          json
        end
        return ret
      end
    end
  end
end
