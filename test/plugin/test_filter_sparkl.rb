#
# Copyright 2017- SPARKL Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
require "helper"
require "fluent/plugin/filter_sparkl.rb"
require "digest"
require "rest-client"
require "json"

class SparklFilterTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
  end

  def create_driver(conf)
    Fluent::Test::Driver::Filter.new(Fluent::Plugin::SparklFilter).configure(conf)
  end

  def filter(config, msgs, tag)
    d = create_driver(config)
    d.run do
      d.feed(tag, msgs)
    end
    d.filtered_records
  end
  
  def messages(time)
    [
      [time, {"foo" => "bar", "message" => "hello world"}],
      [time + 10, {"foo" => "bar", "message" => "sparkl and"}],
      [time + 20, {"foo" => "bar", "message" => "fluentd is"}],
      [time + 30, {"foo" => "bar", "message" => "super useful"}],
    ]
  end
   
  def push_events(config, tag)   
    time = Time.now.to_i
    filter(config, messages(time), tag) + filter(config, messages(time + 100), tag) +
      filter(config, messages(time + 200), tag) + filter(config, messages(time + 300), tag)       
  end

  def verify(records)
    sha256 = Digest::SHA256.new
    
    # grab the first block digest from the start of the chain
    first_block_digest = nil
    records.each do | record |
      if record[".sparkl"]["digest"] != nil
        first_block_digest =
          sha256.hexdigest(
            sha256.hexdigest(record[".sparkl"]["digest"]))
        break
      end
    end
    
    #
    # to chain check, start from the end of the event stream and work backwards.
    # the most recent blockcyphered block determines the event boundary before which 
    # we can be strongly assured of the integrity of the event stream.
    #
    events = []
    block = block_index = last_block_digest = last_chaintxn_received = nil
    
    records.reverse.each do | record |   
      events << record

      sparkl = record[".sparkl"]
      if sparkl["digest"] != nil
        # i.e., this is a chain block, as well as an event
        block = sparkl
      end
      
      run_digest = ""
      if sparkl["index"] == 0
        # this is the first event in the block event set, 
        # so let's check the block
        
        # check running event digest given in block
        events.reverse.each do | event |
          
          # get rid of sparkl meta-data for event hashing
          sparkl = event.delete(".sparkl")
          
          # mangle the event prior to hashing - ensures that hashing is repeatable
          event_mangle = Fluent::Plugin::SparklFilter.mangle event
          
          # update the running digest
          run_digest = sha256.hexdigest(
            sha256.hexdigest(
              run_digest + event_mangle))

          # put meta-data back
          event[".sparkl"] = sparkl          
        end       
        assert_equal(run_digest, block["digest"])
        
        digest_src = block["prevdigest"] + block["digest"]
        block_digest = sha256.hexdigest(
          sha256.hexdigest(
            digest_src))
        
        if last_block_digest != nil                  
          # verify that the saved (from last block) digest 
          # for this block is actually the digest
          assert_equal(block_digest, last_block_digest)       
        end 
        
        # if this is a pubchain block, then...
        if block["get"] != nil
          begin 
            get_url = block["get"]
            chaintxn = JSON.parse(RestClient.get get_url, {accept: :json})
            
            # check that the 'received' time has not be messed with
            assert_equal(chaintxn["received"], block["received"])
            
            chaintxn_received = DateTime.rfc3339(chaintxn["received"]).to_time.to_i
            if last_chaintxn_received != nil
              # check that pubchain 'received' timestamp is monotonically decreasing
              assert(last_chaintxn_received > chaintxn_received)
            end
            last_chaintxn_received = chaintxn_received
        
            # check that the pubchain digest is derived from current block digest
            data_hex = nil
            chaintxn["outputs"].each do | output |
              if output["script_type"] == "null-data"
                data_hex = output["data_hex"]
                break
              end
            end
            assert_not_nil(data_hex)
  
            rmd160 = Digest::RMD160.new
            
            if block_index == nil
              block_index = block["block_index"]
            else
              block_index -= 1  
            end
            assert_equal(block_index, block["block_index"])

            recordkey = rmd160.hexdigest first_block_digest + block_index.to_s
            recordvalue = rmd160.hexdigest block_digest
            recorddigest = recordkey + recordvalue
            assert_equal(recorddigest, data_hex)
          rescue
          end            
        end # pubchained block
        
      last_block_digest = block["prevdigest"] 
      events = []
      
      end # event block
    end # all event records      
  end  

  sub_test_case "chain_check" do
    test_records = []
    
    test "simple" do   
      config = %[
      ]        
      records = push_events(config, "sparkl.1")
      verify(
        records) 
    end
    
    # The next two tests will write to a common
    # chain.  They will only pass if the second
    # test that runs actually extends the chain
    # created by the first.  Otherwise, if there 
    # are two separate chains, the value of 
    # prev_digest will be "" for the first block
    # of the second chain, leading to a different
    # value of last_block_hash used in the 
    # block digest assertion for the last block
    # of the first chain! Also, first_block_digest 
    # and block_index will be different, which 
    # will affect the key stored on the pubchain
    # as well as causing the block_index assertion 
    # to fail.
    # 
    
    test "coinnet_count_check" do   
      config = %[
        interval_type count
        action_interval 2
        coinnet bcy/test
        coinnet_token {{ coinnet_token }}
      ]  
      test_records += 
        push_events(config, "sparkl.2")       
      verify(
        test_records)
    end
    
    test "coinnet_time_check" do   
      config = %[
        interval_type time
        action_interval 120
        coinnet bcy/test
        coinnet_token {{ coinnet_token }}
      ] 
      
      test_records += 
        push_events(config, "sparkl.2")                  
      verify(
        test_records)    
    end
  end  
end
