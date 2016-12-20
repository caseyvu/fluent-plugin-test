require 'spec_helper'
require 'fluent/plugin/geoip'

RSpec.describe Fluent::Geoip do
  let(:dummy_class) do 
    temp = Class.new.extend(Fluent::Geoip)
    temp.set_geoip_db_path('spec/cache/GeoLite2-City.mmdb')
    temp
  end

  let(:us_ip) { '74.125.225.224' }
  let(:sg_ip) { '202.73.39.34' }

  describe 'country_code_of_ip' do

    it 'should return us' do
      expect(dummy_class.country_code_of_ip(us_ip)).to eq('us')
    end

    it 'should return sg' do
      expect(dummy_class.country_code_of_ip(sg_ip)).to eq('sg')
    end
  end

  describe 'city_of_ip' do
    context 'us' do
      let(:record) { dummy_class.city_of_ip(us_ip) }

      it 'should return correct record for us' do
        expect(record['dma_code']).to eq(807)
        expect(record['city_name']).to eq("Mountain View")
        expect(record['latitude']).to eq("37.419200000000004")
        expect(record['longitude']).to eq("-122.0574")
        expect(record['postal_code']).to eq("94043")
        expect(record['region_name']).to eq("CA")
      end

    end
    context 'sg' do
      let(:record) { dummy_class.city_of_ip(sg_ip) }

      it 'should return correct record for sg' do

        expect(record['dma_code']).to be_nil
        expect(record['city_name']).to eq('Singapore')
        expect(record['latitude']).to eq("1.2854999999999999")
        expect(record['longitude']).to eq("103.8565")
        expect(record['postal_code']).to be_nil
        expect(record['region_name']).to eq("01")
      end
    end
  end
end
