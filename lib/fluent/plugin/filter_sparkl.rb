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

require "fluent/plugin/filter"
require "fluent/config/error"
require "fluent/event"
require "fluent/log"
require "digest"
require "rest-client"
require "json"


module Fluent::Plugin
  class SparklFilter < Fluent::Plugin::Filter
    Fluent::Plugin.register_filter("sparkl", self)       
    
    @@mutex = Mutex.new
    @@state = {
      :digest => {},
      :id => {}
    }    
    @@execution_id = "sparkl" + Time.now.to_i.to_s

    #desc "Type of interval for action on chain digests":
    #  "count", "time", or not specified is do nothing
    #  "count" - do action after :action_interval digests
    #  "time" - do action after every :action_interval seconds
    # 
    config_param :interval_type, :string, default: nil

    #desc "Interval between action on chain digests"
    # see :interval_type
    #
    config_param :action_interval, :integer, default: 1000
    
    #desc "Blockcypher coin/network combination"
    # Possible values: bcy/test, btc/main, btc/test3
    # Plugin implementation assumes use of:
    # https://api.blockcypher.com/v1/ api, at this time.
    #
    config_param :coinnet, :string, default: "bcy/test"
    
    #desc "Blockcypher coin/network access token"
    # 
    config_param :coinnet_token, :string, default: nil   

    def configure(conf)
      super
    end

    def filter_stream(tag, es)
      new_es = Fluent::MultiEventStream.new
      sha256 = Digest::SHA256.new      
      run_digest = ""
      
      block_id = nil
      
      id = get_state(:id, tag)
      id[:mutex].synchronize do 
        if not id[:index]
          id[:index] = 0
        end
        block_id = id[:index]
        id[:index] += 1    
      end  
      
      es.each_with_index do |(time, record), index|
        begin
          run_digest = 
            sha256.hexdigest(
              sha256.digest(
                run_digest + SparklFilter.mangle(record)))
          
          record[".sparkl"] = {}
          record[".sparkl"]["index"] = index
          record[".sparkl"]["exec"] = @@execution_id
          record[".sparkl"]["block_id"] = block_id
      
          if (not es.respond_to?("size")) or (index + 1 == es.size)
            record[".sparkl"]["digest"] = run_digest
            chain_block(tag, time, record)
          end
          new_es.add(time, record)
        rescue => e
          router.emit_error_event(tag, time, record, e)
        end
      end
      new_es
    end

    def self.mangle(val) 
      JSON.dump(
       SparklFilter.mangle_(val))
    end

    private  
    
    def self.mangle_(val)
      if val.is_a? Hash
        keys = val.keys
        res = {}
        keys.each do |key|
          res[key] = SparklFilter.mangle_(val[key])
        end
        res.sort
      elsif val.is_a? Array
        res = []
        val.each do |el|
          res << SparklFilter.mangle_(el)    
        end
        res
      elsif val == nil
        ""
      else
        val
      end    
    end
    
    def get_state(which, tag)
      record = {}
      if @@state[which].key?(tag)
        record = @@state[which][tag]
      else  
        @@mutex.synchronize do
          if @@state[which].key?(tag)
            record = @@state[which][tag]
          else
            @@state[which][tag] = record
            record[:mutex] = Mutex.new
          end
        end  
        record
      end    
    end
    
    def chain_block(tag, time, record)
      sha256 = Digest::SHA256.new 
      record[".sparkl"]["class"] = tag
           
      digest = get_state(:digest, tag)
      digest[:mutex].synchronize do 
        if not digest[:last]
          digest[:last] = ""
          digest[:index] = 0
          if @interval_type == "count" 
            digest[:count] = 0
          elsif @interval_type == "time"
            digest[:count] = Time.now.to_i
          end    
        end
        
        record[".sparkl"]["prevdigest"] = digest[:last]
        digest[:last] = 
          sha256.hexdigest(
            sha256.digest(record[".sparkl"]["prevdigest"] + 
              record[".sparkl"]["digest"]))
        if digest[:first] == nil
          digest[:first] = digest[:last] 
        end 

        if @interval_type == "count" 
          digest[:count] += 1
          if digest[:count] == @action_interval
            send_digest(record, tag, digest[:first], digest[:last], digest[:index])
            digest[:count] = 0
            digest[:index] += 1
          end   
        elsif @interval_type == "time"  
          expiry_time = digest[:count] + @action_interval 
          if time >= expiry_time
            send_digest(record, tag, digest[:first], digest[:last], digest[:index])
            digest[:count] = expiry_time
            digest[:index] += 1
          end                    
        end
      end      
    end   
    
    def send_digest(record, tag, first, last, index)      
      rmd160 = Digest::RMD160.new
      recordkey = 
          rmd160.hexdigest first + index.to_s
      recordvalue = 
          rmd160.hexdigest last
      recorddigest = recordkey + recordvalue
      
      body = { "data" => recorddigest }
      uri = "https://api.blockcypher.com/v1/" + @coinnet + "/txs/data?token=" + @coinnet_token
      
      begin
        record[".sparkl"]["block_index"] = index
        txn =  
          JSON.parse(RestClient.post uri, body, {accept: :json})["hash"]
        record[".sparkl"]["get"] = get = 
          "https://api.blockcypher.com/v1/" + @coinnet + "/txs/" + txn
        record[".sparkl"]["received"] = 
          JSON.parse(RestClient.get get, {accept: :json})["received"]
      rescue => e
        log.debug e.to_s
      end      
    end 

  end # class
end # module
