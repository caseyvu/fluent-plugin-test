require 'maxminddb'

module Fluent
  module Geoip
    @@geoip_db_path = '/etc/geoip/GeoLite2-City.mmdb'

    def country_code_of_ip(ip)
      iso_code = global_geoip.lookup(ip).country.iso_code
      iso_code && iso_code.downcase
    end

    def city_of_ip(ip)
      city = global_geoip.lookup(ip)
      record = {}
      record['dma_code'] = city.location.metro_code
      record['city_name'] = city.city && city.city.name
      record['latitude'] = city.location.latitude.to_s
      record['longitude'] = city.location.longitude.to_s
      record['postal_code'] = city.postal.code
      record['region_name'] = city.subdivisions.first && city.subdivisions.first.iso_code
      record
    end

    def global_geoip
      if Time.now.strftime('%M:%S') == '01:00'
        @@global_geoip = ::MaxMindDB.new(geoip_db_path)
      else
        @@global_geoip ||= ::MaxMindDB.new(geoip_db_path)
      end
    end

    def set_geoip_db_path(path)
      @@geoip_db_path = path
    end

    def geoip_db_path
      @@geoip_db_path
    end
  end
end