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
      	router.emit('development', time, record)
      }
    end

    
  end

end

