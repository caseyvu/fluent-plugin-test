require 'fluent/output'
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

      #headers = record['headers']
      #ip = record['ip']
      #params =  if messages['params'].nil? {} 
      #          else messages['params']
      #          end

      router.emit('development', time, messages)

      #status = messages['status']
      #if status == 200
        # Get the timestamp
        #t = get_record_time(params, headers)
        #if t <= 0
        #  t = time.to_i
        #end
        #new_record = params.merge({'ip' => ip, 'path' => messages['path']})

        
      #end
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
    
  end

end

