require 'fluent/output'
require 'fluent/plugin/geoip'

require 'json'

module Fluent
	class VikiOutput < Output 
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

		UNUSED_FIELDS = ['access','sig']

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
      	if filter_record(record)
          process_record(record, time)
        end
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

      # Get the timestamp
      t = get_record_time(params, headers)
      if t <= 0
        t = time.to_i
      end
      new_record = params.merge({'ip' => ip, 't' => t.to_s})

      # Fix other fields
      # TODO
      new_record = clean_up_record(new_record)
      new_record = fix_xunlei(new_record)

      # emit new record
      router.emit(resolve_tag(messages['path']), time, new_record)
    end


    # Should only accept record with status == 200 
    def filter_record(record)
    	record['status'] == 200
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

    # Cleaning some fields
    def clean_up_record(record)
      record['uuid'] = record.delete('viki_uuid') if record['viki_uuid']
      record['content_provider'] = record.delete('type') if record['type']
      record['device_id'] = record.delete('dev_model') if record['dev_model']
      record.each { |k, v| record[k] = '' if v == 'null' }
      # rename video_view to minute_view
      record['event'] = 'minute_view' if record['event'] == 'video_view'
      record
    end

    # Fix xunlei record
    def fix_xunlei(record)
      # fix xunlei data sending timestamps
      if record['app_id'] == '100105a'
        record.delete_if {|key, _|  !!(key =~ /\A[0-9]+{13}\z/) }
      end
      record
    end

    # TODO
    def set_record_domain(record)
    	site = record['site']
      record['domain'] = site.gsub(/^https?:\/\//, '').gsub(/([^\/]*)(.*)/, '\1').gsub(/^www\./, '') if site
    end

    # TODO
    # Check the IP address, set the country
    def set_record_ip(record)
    	params = record['params']
    	record['ip'] ||= params['HTTP_X_FORWARDED_FOR'] || params['REMOTE_ADDR']
      ips = unless record['ip'].nil?
        if record['ip'].kind_of? String
          record['ip'].gsub(' ', ',').split(',')
        elsif record['ip'].kind_of? Array
          record['ip']
            .map {|e| e.split(",").map(&:strip) }
            .inject([]) {|accum, e| accum + e}
        end
      end

      record['country'] = record.delete('country_code') if record['country_code']

      valid_ip, geo_country = resolve_correct_ip(ips)

      record['ip_raw'], record['ip'] = record['ip'], valid_ip
      record['country'] = geo_country || record['country']

      record.merge! city_of_ip(valid_ip)
    end

    # TODO
    def resolve_correct_ip(ips)
      ips.each do |ip|
        geo_country = country_code_of_ip(ip)

        return [ip, geo_country] unless geo_country.nil?
      end unless ips.nil?

      [ips.first, nil]
    end

    
    # TODO
    # Get unique event ID
    def set_unique_id(record)
      # generate a unique event id for each event
      #unless record['mid']
      #  record['mid'] = params['HTTP_X_REQUEST_ID'] || gen_message_id(time)
      #end
    end

    # TODO
    def update_subtitles(record)
    	# rename bottom_subtitle to subtitle_lang, add subtitle_enabled
      #if %w(video_play minute_view).include?(record['event'])
      #  update_subtitles(record)
      #end
    end

  end

end

