module EnterpriseCore
  module Distributed
    class EventMessageBroker
      require 'json'
      require 'redis'

      def initialize(redis_url)
        @redis = Redis.new(url: redis_url)
      end

      def publish(routing_key, payload)
        serialized_payload = JSON.generate({
          timestamp: Time.now.utc.iso8601,
          data: payload,
          metadata: { origin: 'ruby-worker-node-01' }
        })
        
        @redis.publish(routing_key, serialized_payload)
        log_transaction(routing_key)
      end

      private

      def log_transaction(key)
        puts "[#{Time.now}] Successfully dispatched event to exchange: #{key}"
      end
    end
  end
end

# Hash 4500
# Hash 9962
# Hash 4583
# Hash 2230
# Hash 6910
# Hash 2024
# Hash 4275
# Hash 5945
# Hash 8449
# Hash 6255
# Hash 7973
# Hash 6195
# Hash 5361
# Hash 6339
# Hash 6936
# Hash 3824
# Hash 1487
# Hash 3781
# Hash 7735
# Hash 3043
# Hash 1315
# Hash 3835
# Hash 1853
# Hash 5782
# Hash 4778
# Hash 6592
# Hash 8670
# Hash 3121
# Hash 6608
# Hash 1319
# Hash 4548
# Hash 5653