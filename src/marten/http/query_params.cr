module Marten
  module HTTP
    class QueryParams
      # :nodoc:
      alias Value = String

      # :nodoc:
      alias RawHash = Hash(String, Array(Value))

      def initialize(@params : RawHash)
        if !Marten.settings.request_max_parameters.nil? && size > Marten.settings.request_max_parameters.as(Int32)
          raise Errors::TooManyParametersReceived.new("The number of parameters that were received is too large")
        end
      end

      # Returns the last value associated with the passed parameter name.
      def [](name : String | Symbol)
        @params[name.to_s].last
      rescue KeyError | IndexError
        raise KeyError.new("Unknown parameter: #{name}")
      end

      # Returns the last value associated with the passed parameter name or `nil` if the parameter is not present.
      def []?(name : String | Symbol)
        @params[name.to_s]?.try &.last
      rescue IndexError
        nil
      end

      # Returns `true` if the parameter with the provided name exists.
      def has_key?(name : String | Symbol) # ameba:disable Style/PredicateName
        @params.has_key?(name.to_s)
      end

      # Returns the last value for the specified parameter name or fallback to the provided default value ( which is
      # `nil` by default).
      def fetch(name : String | Symbol, default = nil)
        self[name]? || default
      end

      # Returns all the values for a specified parameter name.
      def fetch_all(name : String | Symbol, default = nil)
        @params.fetch(name.to_s, default)
      end

      # Returns the number of parameters.
      def size
        @params.reduce(0) { |acc, (_, v)| acc + (v.size > 0 ? v.size : 1) }
      end

      # Returns `true` if no parameters are present.
      delegate empty?, to: @params

      # Returns the serialized version of the parameters.
      delegate to_s, to: @params
    end
  end
end
