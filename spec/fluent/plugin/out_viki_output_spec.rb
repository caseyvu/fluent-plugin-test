require 'spec_helper'

RSpec.describe Fluent::VikiOutput do

  before(:all) { Fluent::Test.setup }
  let(:now) { Time.parse("2011-01-02 13:14:15 UTC") }
  let(:now_ts) { now.to_i }

  let(:driver) { Fluent::Test::OutputTestDriver.new(described_class) }

  describe 'out_viki_output' do
  	it 'filter out all non-200 requests' do
  		Timecop.freeze(now) do
  			driver.run do
  				driver.emit({'headers' => {},'messages' => {'status' => 200,'params' => {'event' => 'test1'}}.to_json})
  				driver.emit({'headers' => {},'messages' => {'status' => 400,'params' => {'event' => 'test'}}.to_json})
  				driver.emit({'headers' => {},'messages' => {'status' => 401,'params' => {'event' => 'test'}}.to_json})
  				driver.emit({'headers' => {},'messages' => {'status' => 500,'params' => {'event' => 'test'}}.to_json})
  				driver.emit({'headers' => {},'messages' => {'status' => 200,'params' => {'event' => 'test2'}}.to_json})
  			end
  			driver.expect_emit 'unknown', now_ts, {'event' =>'test1'}
  			driver.expect_emit 'unknown', now_ts, {'event' =>'test2'}
  		end
  	end

  	it 'tags are extracted from path' do
  		Timecop.freeze(now) do
  			driver.run do
  				driver.emit({'headers' => {},'messages' => {'status' => 200,'path' => '/api/production','params' => {'event' => 'test1'}}.to_json})
  				driver.emit({'headers' => {},'messages' => {'status' => 200,'path' => '/api/development','params' => {'event' => 'test2'}}.to_json})
  				driver.emit({'headers' => {},'messages' => {'status' => 200,'path' => 'something','params' => {'event' => 'test3'}}.to_json})
  				driver.emit({'headers' => {},'messages' => {'status' => 200,'path' => '','params' => {'event' => 'test4'}}.to_json})
  				driver.emit({'headers' => {},'messages' => {'status' => 200,'params' => {'event' => 'test5'}}.to_json})
  			end
  			driver.expect_emit 'production', now_ts, {'event' =>'test1'}
  			driver.expect_emit 'development', now_ts, {'event' =>'test2'}
  			driver.expect_emit 'unknown', now_ts, {'event' =>'test3'}
  			driver.expect_emit 'unknown', now_ts, {'event' =>'test4'}
  			driver.expect_emit 'unknown', now_ts, {'event' =>'test5'}
  		end
  	end
  end
end


