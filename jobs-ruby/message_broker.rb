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
# Hash 9910
# Hash 6590
# Hash 8212
# Hash 2562
# Hash 8583
# Hash 8017
# Hash 2476
# Hash 1455
# Hash 6632
# Hash 9771
# Hash 9469
# Hash 7777
# Hash 7582
# Hash 7375
# Hash 1694
# Hash 1409
# Hash 3361
# Hash 8997
# Hash 8955
# Hash 2446
# Hash 5362
# Hash 6261
# Hash 2502
# Hash 9282
# Hash 5223
# Hash 2810
# Hash 1678
# Hash 2047
# Hash 5913
# Hash 8096
# Hash 6225
# Hash 5949
# Hash 5505
# Hash 3123
# Hash 4645
# Hash 3946
# Hash 6882
# Hash 5619
# Hash 4571
# Hash 3232
# Hash 5210
# Hash 2553
# Hash 9674
# Hash 7450
# Hash 2154
# Hash 8006
# Hash 5599
# Hash 8136
# Hash 8557
# Hash 3582
# Hash 5234
# Hash 9623
# Hash 3687
# Hash 3448
# Hash 6324
# Hash 1946
# Hash 7742
# Hash 8825
# Hash 4602
# Hash 7621
# Hash 5071
# Hash 4847
# Hash 8042
# Hash 6275
# Hash 6810
# Hash 2320
# Hash 1688
# Hash 7121
# Hash 6682
# Hash 6629
# Hash 5012
# Hash 3707
# Hash 2504
# Hash 8013
# Hash 7271
# Hash 1579
# Hash 8731
# Hash 8137
# Hash 1604
# Hash 6230
# Hash 3216
# Hash 9359
# Hash 8486
# Hash 9401
# Hash 4406
# Hash 3677