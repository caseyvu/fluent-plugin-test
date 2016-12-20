require 'fluent/output'
require 'fluent/plugin/geoip'
require 'json'

module Fluent
	class VikiOutput < Output 
    include Geoip

		Fluent::Plugin.register_output('viki_output', self)

		# To support Fluentd v0.10.57 or earlier
  	unless method_defined?(:router)
    	define_method("router") { Fluent::Engine }
  	end

  	# For fluentd v0.12.16 or earlier
  	class << self
    	unless method_defined?(:desc)
      	def desc(description)
      	end
    	end
  	end

		def configure(conf)
      super
    end

    def start
      super
    end

    def shutdown
      super
    end

    def emit(tag, es, chain)
      chain.next
      es.each {|time,record|
      	process_record(record, time)
      }
    end

    private
    def process_record(record, time)
      messages =  begin
                    messages = JSON.parse(record['messages'])
                  rescue JSON::ParserError => e
                    {}
                  end

      headers = record['headers']
      ip = record['ip']
      params =  if messages['params'].nil? {} 
                else messages['params']
                end

      if filter_record(messages)
        # Get the timestamp
        t = get_record_time(params, headers)
        if t <= 0
          t = time.to_i
        end
        new_record = params.merge({'t' => t.to_s, 'path' => messages['path']})

        # Fix IP, find country and other info from IP address
        set_record_ip(ip, headers, new_record)
        # Set domain from site
        set_record_domain(new_record)
        # Clean up record
        clean_up_record(new_record)
        # Generate unique ID
        set_unique_id(new_record, headers, time)
        # Fix subtitle-related fields
        update_subtitles(new_record)
        # Fix xunlei record
        fix_xunlei(new_record)

        router.emit(resolve_tag(messages['path']), time, new_record)
      end
    end

    # Should only accept record with status == 200 
    def filter_record(messages)
      if messages.nil?
        return false
      end
      return messages['status'] == 200
    end

    # Get tag for record
    def resolve_tag(path)
      if path == '/api/production'
        'production'
      elsif path == '/api/development'
        'development'
      else
        'unknown'
      end
    end

    # Fix the time: using time from the record if available
    def get_record_time(params, headers)
      if headers['HTTP_TIMESTAMP']
        return headers['HTTP_TIMESTAMP'].to_i
      elsif headers['TIMESTAMP']
        return headers['TIMESTAMP'].to_i
      else
        return params['t'].to_i
      end
    end

    # Cleaning some fields
    def clean_up_record(record)
      record['uuid'] = record.delete('viki_uuid') if record['viki_uuid']
      record['content_provider'] = record.delete('type') if record['type']
      record['device_id'] = record.delete('dev_model') if record['dev_model']
      record.each { |k, v| record[k] = '' if v == 'null' }
      # rename video_view to minute_view
      record['event'] = 'minute_view' if record['event'] == 'video_view'
    end

    # Fix xunlei record
    def fix_xunlei(record)
      # fix xunlei data sending timestamps
      if record['app_id'] == '100105a'
        record.delete_if {|key, _|  !!(key =~ /\A[0-9]+{13}\z/) }
      end
    end

    # Set domain
    def set_record_domain(record)
      site = record['site']
      record['domain'] = site.gsub(/^https?:\/\//, '').gsub(/([^\/]*)(.*)/, '\1').gsub(/^www\./, '') if site
    end

    # Set unique ID (mid)
    def set_unique_id(record, headers, time)
      # generate a unique event id for each event
      unless record['mid']
        record['mid'] = headers['HTTP_X_REQUEST_ID'] || gen_message_id(time)
      end
    end

    # Generate a unique id for the event, length: 10+1+5 = 16
    # It's relatively sortable
    def gen_message_id(time)
      r = rand(36**5).to_s(36)
      "#{time.to_s}-#{r}"
    end

    # Fix subtile fields
    def update_subtitles(record)
      if %w(video_play minute_view).include?(record['event'])
        record['subtitle_lang'] = record.delete('bottom_subtitle') if record['bottom_subtitle'] and record['subtitle_lang'].nil?

        if record['subtitle_visible'].nil?
          record['subtitle_visible'] = record['subtitle_lang'] && record['subtitle_lang'].size > 0
        end

        # manual subtitle set to Chinese for xunlei and letv
        if %w(100106a 100105a).include?(record['app_id'])
          record['subtitle_enabled'] = true
          record['subtitle_lang'] = 'zh'
        end
      end
    end

    # Find valid IP address and other info from geoip
    def set_record_ip(ip, headers, record)
      raw_ip = record['ip'] || headers['HTTP_X_FORWARDED_FOR'] || headers['REMOTE_ADDR'] || ip
      ips = unless raw_ip.nil?
        if raw_ip.kind_of? String
          raw_ip.gsub(' ', ',').split(',')
        elsif raw_ip.kind_of? Array
          raw_ip
            .map {|e| e.split(",").map(&:strip) }
            .inject([]) {|accum, e| accum + e}
        end
      end

      record['country'] = record.delete('country_code') if record['country_code']

      valid_ip, geo_country = resolve_correct_ip(ips)

      record['ip_raw'], record['ip'] = raw_ip, valid_ip
      record['country'] = geo_country || record['country']

      record.merge! city_of_ip(valid_ip)
    end

    def resolve_correct_ip(ips)
      ips.each do |ip|
        geo_country = country_code_of_ip(ip)

        return [ip, geo_country] unless geo_country.nil?
      end unless ips.nil?

      [ips.first, nil]
    end

    
  end

end

