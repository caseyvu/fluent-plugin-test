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
  				driver.emit({'status' => 200,'headers' => {},'messages' => {'params' => {'event' => 'test1'}}.to_json})
  				driver.emit({'status' => 400,'headers' => {},'messages' => {'params' => {'event' => 'test'}}.to_json})
  				driver.emit({'status' => 401,'headers' => {},'messages' => {'params' => {'event' => 'test'}}.to_json})
  				driver.emit({'status' => 500,'headers' => {},'messages' => {'params' => {'event' => 'test'}}.to_json})
  				driver.emit({'status' => 200,'headers' => {},'messages' => {'params' => {'event' => 'test2'}}.to_json})
  			end
  			driver.expect_emit 'unknown', now_ts, {'event' =>'test1'}
  			driver.expect_emit 'unknown', now_ts, {'event' =>'test2'}
  		end
  	end

  	it 'tags are extracted from path' do
  		Timecop.freeze(now) do
  			driver.run do
  				driver.emit({'status' => 200,'headers' => {},'messages' => {'path' => '/api/production','params' => {'event' => 'test1'}}.to_json})
  				driver.emit({'status' => 200,'headers' => {},'messages' => {'path' => '/api/development','params' => {'event' => 'test2'}}.to_json})
  				driver.emit({'status' => 200,'headers' => {},'messages' => {'path' => 'something','params' => {'event' => 'test3'}}.to_json})
  				driver.emit({'status' => 200,'headers' => {},'messages' => {'path' => '','params' => {'event' => 'test4'}}.to_json})
  				driver.emit({'status' => 200,'headers' => {},'messages' => {'params' => {'event' => 'test5'}}.to_json})
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