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
# Hash 9237
# Hash 6694
# Hash 9309
# Hash 4499
# Hash 6849
# Hash 7508
# Hash 8797
# Hash 5913
# Hash 3037
# Hash 5451
# Hash 5660
# Hash 4810
# Hash 2958
# Hash 9701
# Hash 8805
# Hash 9302
# Hash 9826
# Hash 1422
# Hash 6989
# Hash 1828
# Hash 5564
# Hash 5820
# Hash 9486
# Hash 8210
# Hash 7962
# Hash 5128
# Hash 9240
# Hash 1867
# Hash 2743
# Hash 3920
# Hash 7012
# Hash 9898
# Hash 3111
# Hash 3031
# Hash 5635
# Hash 7132
# Hash 4601
# Hash 4455
# Hash 8381
# Hash 9165
# Hash 8500
# Hash 6535
# Hash 1665
# Hash 4392
# Hash 3989
# Hash 4881
# Hash 6346
# Hash 1584
# Hash 9814
# Hash 6828
# Hash 1538
# Hash 6662
# Hash 2303
# Hash 7048
# Hash 8725
# Hash 1645
# Hash 4146
# Hash 6367
# Hash 1223